// Command riverbridge builds the CLIProxyAPI plugin shared library.
//
// The plugin exposes the cliproxy plugin ABI (a small C surface) and forwards
// every call into internal/bridge, which speaks JSON over that surface.
package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);

static cliproxy_host_api stored_host;
static int host_ready;

static void store_host_api(const cliproxy_host_api* host) {
	if (host == NULL) {
		host_ready = 0;
		return;
	}
	stored_host = *host;
	host_ready = 1;
}

static int call_host_api(const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	if (!host_ready || stored_host.call == NULL) {
		return 1;
	}
	return stored_host.call(stored_host.host_ctx, method, request, request_len, response);
}

static void free_host_buffer(void* ptr, size_t len) {
	if (host_ready && stored_host.free_buffer != NULL && ptr != NULL) {
		stored_host.free_buffer(ptr, len);
	}
}
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"unsafe"

	"github.com/himwei/cpa-plugin-riverbridge/internal/bridge"
)

const abiVersion = 1

type rpcEnvelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	ErrorCode  string `json:"code"`
	Message    string `json:"message"`
	Retry      bool   `json:"retryable,omitempty"`
	HTTPStatus int    `json:"http_status,omitempty"`
}

func (e *rpcError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func (e *rpcError) StatusCode() int {
	if e == nil {
		return 0
	}
	return e.HTTPStatus
}

func (e *rpcError) Code() string {
	if e == nil {
		return ""
	}
	return e.ErrorCode
}

func (e *rpcError) Retryable() bool {
	return e != nil && e.Retry
}

var (
	service   *bridge.Service
	hostMu    sync.RWMutex
	libraryMu sync.RWMutex
)

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) (rc C.int) {
	libraryMu.Lock()
	defer libraryMu.Unlock()
	defer func() {
		if recover() != nil {
			rc = 1
		}
	}()
	if host == nil || host.abi_version != C.uint32_t(abiVersion) || host.call == nil ||
		host.free_buffer == nil || plugin == nil {
		return 1
	}
	if service != nil {
		return 1
	}
	hostMu.Lock()
	C.store_host_api(host)
	hostMu.Unlock()
	service = bridge.NewService()
	service.SetHost(callHost)
	plugin.abi_version = C.uint32_t(abiVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) (rc C.int) {
	libraryMu.RLock()
	defer libraryMu.RUnlock()
	if response == nil {
		return 1
	}
	response.ptr = nil
	response.len = 0
	defer func() {
		if recover() != nil {
			writeResponse(response, failure(errorDetails(&rpcError{
				ErrorCode:  "plugin_panic",
				Message:    "plugin callback panicked",
				HTTPStatus: http.StatusInternalServerError,
			})))
			rc = 1
		}
	}()
	if method == nil || strings.TrimSpace(C.GoString(method)) == "" {
		writeResponse(response, failure(&rpcError{
			ErrorCode:  "invalid_method",
			Message:    "method is required",
			HTTPStatus: http.StatusBadRequest,
		}))
		return 1
	}
	if requestLen > C.size_t(math.MaxInt32) || (requestLen > 0 && request == nil) {
		writeResponse(response, failure(&rpcError{
			ErrorCode:  "invalid_request",
			Message:    "request buffer is invalid",
			HTTPStatus: http.StatusBadRequest,
		}))
		return 1
	}
	var raw json.RawMessage
	if requestLen > 0 {
		raw = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
		if !json.Valid(raw) {
			writeResponse(response, failure(&rpcError{
				ErrorCode:  "invalid_request",
				Message:    "request JSON is invalid",
				HTTPStatus: http.StatusBadRequest,
			}))
			return 1
		}
	}
	if service == nil {
		writeResponse(response, failure(&rpcError{
			ErrorCode:  "plugin_stopped",
			Message:    "plugin is not initialized",
			HTTPStatus: http.StatusServiceUnavailable,
		}))
		return 1
	}
	result, err := service.Handle(C.GoString(method), raw)
	if err != nil {
		writeResponse(response, failure(errorDetails(err)))
		return 1
	}
	encoded, err := success(result)
	if err != nil {
		writeResponse(response, failure(&rpcError{
			ErrorCode:  "serialization_error",
			Message:    "plugin response cannot be encoded",
			HTTPStatus: http.StatusInternalServerError,
		}))
		return 1
	}
	writeResponse(response, encoded)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, _ C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {
	libraryMu.Lock()
	defer libraryMu.Unlock()
	defer func() {
		service = nil
		hostMu.Lock()
		C.store_host_api(nil)
		hostMu.Unlock()
		_ = recover()
	}()
	if service != nil {
		_, _ = service.Handle("plugin.shutdown", nil)
	}
}

func success(result any) ([]byte, error) {
	if result == nil {
		result = struct{}{}
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return json.Marshal(rpcEnvelope{OK: true, Result: raw})
}

func failure(details *rpcError) []byte {
	raw, err := json.Marshal(rpcEnvelope{OK: false, Error: details})
	if err != nil {
		return []byte(`{"ok":false,"error":{"code":"serialization_error",` +
			`"message":"plugin error cannot be encoded","http_status":500}}`)
	}
	return raw
}

func errorDetails(err error) *rpcError {
	message, status := bridge.HostErrorDetails(err)
	details := &rpcError{ErrorCode: "plugin_error", Message: message, HTTPStatus: status}
	var code interface{ Code() string }
	if errors.As(err, &code) && strings.TrimSpace(code.Code()) != "" {
		details.ErrorCode = code.Code()
	}
	var retry interface{ Retryable() bool }
	if errors.As(err, &retry) {
		details.Retry = retry.Retryable()
	}
	return details
}

func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	if ptr == nil {
		return
	}
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}

func callHost(method string, payload any, out any) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("host callback %s panicked", method)
		}
	}()
	if strings.TrimSpace(method) == "" {
		return fmt.Errorf("host callback method is required")
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode host callback %s: %w", method, err)
	}
	if len(rawPayload) > math.MaxInt32 {
		return fmt.Errorf("host callback %s request is too large", method)
	}
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))
	cPayload := C.CBytes(rawPayload)
	defer C.free(cPayload)
	rawResponse, callCode, err := invokeHost(cMethod, (*C.uint8_t)(cPayload), C.size_t(len(rawPayload)))
	if err != nil {
		return fmt.Errorf("host callback %s: %w", method, err)
	}
	var envelope rpcEnvelope
	if err := json.Unmarshal(rawResponse, &envelope); err != nil {
		return fmt.Errorf("decode host callback %s: %w", method, err)
	}
	if callCode != 0 {
		if envelope.Error != nil {
			return envelope.Error
		}
		return fmt.Errorf("host callback %s returned code=%d", method, int(callCode))
	}
	if !envelope.OK {
		if envelope.Error != nil {
			return envelope.Error
		}
		return fmt.Errorf("host callback %s failed", method)
	}
	if out != nil && len(envelope.Result) > 0 {
		if err := json.Unmarshal(envelope.Result, out); err != nil {
			return fmt.Errorf("decode host callback %s result: %w", method, err)
		}
	}
	return nil
}

func invokeHost(method *C.char, payload *C.uint8_t, length C.size_t) ([]byte, C.int, error) {
	hostMu.RLock()
	defer hostMu.RUnlock()
	var response C.cliproxy_buffer
	code := C.call_host_api(method, payload, length, &response)
	if response.ptr == nil || response.len == 0 {
		return []byte(`{"ok":true}`), code, nil
	}
	raw := C.GoBytes(response.ptr, C.int(response.len))
	C.free_host_buffer(response.ptr, response.len)
	return raw, code, nil
}
