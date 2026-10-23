package channel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/store"
)

const adminPrefix = "/api/v1/admin/channels"
const createJSON = `{"name":"n","provider":"openai","base_url":"http://localhost","models":[{"model":"a","upstream_model":"up"}],"keys":["private-key"]}`
const updateJSON = `{"name":"n","provider":"openai","base_url":"http://localhost","priority":0,"weight":1,"enabled":false,"models":[{"model":"a","upstream_model":"up"}]}`

type httpServiceFake struct {
	calls  atomic.Int32
	err    error
	input  CreateChannelInput
	update UpdateChannelInput
	detail *ChannelDetail
}

func (f *httpServiceFake) List(context.Context) ([]ChannelView, error) {
	f.calls.Add(1)
	return []ChannelView{}, f.err
}
func (f *httpServiceFake) Get(context.Context, int64) (*ChannelDetail, error) {
	f.calls.Add(1)
	return f.detail, f.err
}
func (f *httpServiceFake) Create(_ context.Context, in CreateChannelInput) (*ChannelDetail, error) {
	f.calls.Add(1)
	f.input = in
	return f.detail, f.err
}
func (f *httpServiceFake) Update(_ context.Context, _ int64, in UpdateChannelInput) (*ChannelDetail, error) {
	f.calls.Add(1)
	f.update = in
	return f.detail, f.err
}
func (f *httpServiceFake) Delete(context.Context, int64) error { f.calls.Add(1); return f.err }
func (f *httpServiceFake) AddKey(context.Context, int64, string) (*KeyView, error) {
	f.calls.Add(1)
	return &KeyView{ID: 2, ChannelID: 1, Weight: 1, Status: 1}, f.err
}
func (f *httpServiceFake) UpdateKey(_ context.Context, _, _ int64, w int32, status int8) (*KeyView, error) {
	f.calls.Add(1)
	return &KeyView{ID: 2, ChannelID: 1, Weight: w, Status: status}, f.err
}
func (f *httpServiceFake) RemoveKey(context.Context, int64, int64) error {
	f.calls.Add(1)
	return f.err
}

type testerFake struct {
	calls atomic.Int32
	test  func(context.Context, *store.Channel, string) (ProbeResult, error)
}

func (f *testerFake) Test(ctx context.Context, ch *store.Channel, model string) (ProbeResult, error) {
	f.calls.Add(1)
	if f.test != nil {
		return f.test(ctx, ch, model)
	}
	return ProbeResult{OK: true, StatusCode: 200, Reason: "success"}, nil
}

type adminIdentityKey struct{}

func httpRouter(service AdminService, tester ConnectivityTester) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.RequestID())
	RegisterAdminRoutes(r, func(c *gin.Context) {
		if c.GetHeader("Authorization") == "" {
			writeAdminEnvelope(c, 401, "gaigate_unauthorized", "invalid API Key")
			return
		}
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), adminIdentityKey{}, c.GetHeader("Authorization") == "admin"))
		c.Next()
	}, func(ctx context.Context) bool { admin, _ := ctx.Value(adminIdentityKey{}).(bool); return admin }, service, tester)
	return r
}

func httpFake() *httpServiceFake {
	return &httpServiceFake{detail: &ChannelDetail{ChannelView: ChannelView{ID: 1, Provider: "openai", BaseURL: "http://localhost", Enabled: false},
		Models: []ModelMappingInput{{Model: "a", UpstreamModel: "up"}}, Keys: []KeyView{}}}
}

func adminRequest(r http.Handler, method, path, body, identity, media string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, adminPrefix+path, strings.NewReader(body))
	request.Header.Set("Authorization", identity)
	request.Header.Set("Content-Type", media)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, request)
	return w
}

var adminRouteCases = []struct {
	method, path, body string
	status             int
}{
	{"GET", "", "", 200}, {"GET", "/1", "", 200}, {"POST", "", createJSON, 201}, {"PUT", "/1", updateJSON, 200},
	{"DELETE", "/1", "", 204}, {"POST", "/1/keys", `{"key":"private-key"}`, 201},
	{"PUT", "/1/keys/2", `{"weight":10000,"status":0}`, 200}, {"DELETE", "/1/keys/2", "", 204}, {"POST", "/1/test", `{"model":"a"}`, 200},
}

type countingBody struct {
	reader io.Reader
	read   int
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += n
	return n, err
}
func (*countingBody) Close() error { return nil }

func TestAdminHTTPAllRoutesIdentityPrecedesBodyAndBusiness(t *testing.T) {
	for _, route := range adminRouteCases {
		for _, identity := range []string{"", "user"} {
			t.Run(route.method+route.path+identity, func(t *testing.T) {
				s, tester := httpFake(), &testerFake{}
				r := httpRouter(s, tester)
				body := &countingBody{reader: strings.NewReader(strings.Repeat("x", adminBodyLimit+10))}
				request := httptest.NewRequest(route.method, adminPrefix+route.path, nil)
				request.Body = body
				request.Header.Set("Authorization", identity)
				request.Header.Set("X-Role", "admin")
				w := httptest.NewRecorder()
				r.ServeHTTP(w, request)
				assert.Equal(t, map[string]int{"": 401, "user": 403}[identity], w.Code)
				assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
				assert.Zero(t, body.read)
				assert.Zero(t, s.calls.Load())
				assert.Zero(t, tester.calls.Load())
				assert.Contains(t, w.Body.String(), w.Header().Get("X-Request-Id"))
				if identity == "user" {
					assert.Contains(t, w.Body.String(), "gaigate_forbidden")
				}
			})
		}
	}
}

func TestAdminHTTPSuccessAndDefaults(t *testing.T) {
	for _, route := range adminRouteCases {
		t.Run(route.method+route.path, func(t *testing.T) {
			s, tester := httpFake(), &testerFake{}
			w := adminRequest(httpRouter(s, tester), route.method, route.path, route.body, "admin", "application/json; charset=utf-8")
			assert.Equal(t, route.status, w.Code)
			assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			assert.NotContains(t, w.Body.String(), "private-key")
			if route.status == 204 {
				assert.Empty(t, w.Body.String())
			}
			if route.method == "GET" && route.path == "" {
				assert.JSONEq(t, `{"data":[]}`, w.Body.String())
			}
			if route.method == "POST" && route.path == "" {
				assert.True(t, s.input.Enabled)
				assert.Equal(t, int32(1), s.input.Weight)
				assert.Zero(t, s.input.Priority)
			}
			if route.method == "PUT" && route.path == "/1" {
				assert.False(t, s.update.Enabled)
				assert.Zero(t, s.update.Priority)
			}
			if route.path == "/1/test" {
				assert.Equal(t, int32(1), tester.calls.Load())
			}
		})
	}
	s := httpFake()
	body := strings.TrimSuffix(createJSON, "}") + `,"priority":0,"weight":10000,"enabled":false}`
	w := adminRequest(httpRouter(s, &testerFake{}), "POST", "", body, "admin", "application/json")
	require.Equal(t, 201, w.Code)
	assert.False(t, s.input.Enabled)
	assert.Equal(t, int32(10000), s.input.Weight)
}

func TestAdminHTTPStrictInputsDoNotCallServices(t *testing.T) {
	cases := []struct{ method, path, body string }{
		{"POST", "", ""}, {"POST", "", "null"}, {"POST", "", "[]"}, {"POST", "", "{}"},
		{"POST", "", createJSON + "{}"}, {"POST", "", createJSON + " true"},
		{"POST", "", strings.TrimSuffix(createJSON, "}") + `,"id":1}`},
		{"POST", "", strings.TrimSuffix(createJSON, "}") + `,"name":"duplicate"}`},
		{"POST", "", strings.TrimSuffix(createJSON, "}") + `,"\u006eame":"duplicate"}`},
		{"POST", "", strings.Replace(createJSON, `"model":"a"`, `"model":"a","\u006dodel":"a"`, 1)},
		{"POST", "", strings.Replace(createJSON, `"model":"a"`, `"model":"a","channel_id":1`, 1)},
		{"POST", "", strings.Replace(createJSON, `{"model":"a","upstream_model":"up"}`, "null", 1)},
		{"POST", "", strings.Replace(createJSON, `["private-key"]`, `[null]`, 1)},
		{"POST", "", strings.Replace(createJSON, `["private-key"]`, `[1]`, 1)},
		{"POST", "", strings.Replace(createJSON, `"name":"n"`, `"name":"`+"\xff"+`"`, 1)},
		{"PUT", "/1", createJSON}, {"PUT", "/1", "{}"},
		{"POST", "/1/keys", `{"key":null}`}, {"POST", "/1/keys", `{"key":"a","key":"b"}`},
		{"POST", "/1/keys", `{"key":"a","weight":1}`}, {"POST", "/1/keys", `{"key":"bad\u0000"}`},
		{"PUT", "/1/keys/2", `{"weight":1}`}, {"PUT", "/1/keys/2", `{"weight":1,"status":1,"key":"new"}`},
		{"POST", "/1/test", `{"model":"a","messages":[]}`}, {"POST", "/1/test", `{"model":null}`},
		{"POST", "/1/test", `{"model":"a","\u006dodel":"a"}`}, {"POST", "/1/test", `{"model":1}`},
	}
	// Required/null/type checks apply independently to every complete PUT field.
	var full map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(updateJSON), &full))
	for _, field := range configFields {
		for _, replacement := range []string{"missing", "null", "[]"} {
			copyFields := make(map[string]json.RawMessage)
			for k, v := range full {
				copyFields[k] = v
			}
			if replacement == "missing" {
				delete(copyFields, field)
			} else {
				copyFields[field] = json.RawMessage(replacement)
			}
			body, err := json.Marshal(copyFields)
			require.NoError(t, err)
			cases = append(cases, struct{ method, path, body string }{"PUT", "/1", string(body)})
		}
	}
	for _, field := range []string{"priority", "weight"} {
		for _, value := range []string{"1.0", "1e0", "2147483648", "true", `"1"`} {
			cases = append(cases, struct{ method, path, body string }{"POST", "", strings.TrimSuffix(createJSON, "}") + `,"` + field + `":` + value + `}`})
		}
	}
	for _, body := range []string{`{"weight":0,"status":1}`, `{"weight":10001,"status":1}`, `{"weight":1,"status":2}`, `{"weight":1,"status":128}`, `{"weight":null,"status":1}`, `{"weight":1,"status":null}`, `{"weight":1,"status":1.0}`} {
		cases = append(cases, struct{ method, path, body string }{"PUT", "/1/keys/2", body})
	}
	for _, id := range []string{"0", "-1", "+1", "9223372036854775808", "no-id"} {
		cases = append(cases, struct{ method, path, body string }{"GET", "/" + id, ""}, struct{ method, path, body string }{"DELETE", "/1/keys/" + id, ""})
	}
	for _, tc := range cases {
		t.Run(tc.method+tc.path+"/"+strconvLabel(tc.body), func(t *testing.T) {
			s, tester := httpFake(), &testerFake{}
			w := adminRequest(httpRouter(s, tester), tc.method, tc.path, tc.body, "admin", "application/json")
			assert.Equal(t, 400, w.Code)
			assert.Zero(t, s.calls.Load(), "invalid request must not reach service")
			assert.Zero(t, tester.calls.Load())
			assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
		})
	}
}

func strconvLabel(body string) string { // Test labels omit all potentially sensitive input values.
	return strings.Repeat("x", min(len(body), 5))
}

func TestAdminHTTPBoundedBodyAndMedia(t *testing.T) {
	for _, route := range adminRouteCases {
		if route.method != "POST" && route.method != "PUT" {
			continue
		}
		for _, size := range []int{adminBodyLimit, adminBodyLimit + 1} {
			s, tester := httpFake(), &testerFake{}
			r := httpRouter(s, tester)
			body := route.body + strings.Repeat(" ", size-len(route.body))
			w := adminRequest(r, route.method, route.path, body, "admin", "application/json")
			if size == adminBodyLimit {
				assert.Equal(t, route.status, w.Code)
			} else {
				assert.Equal(t, 413, w.Code)
				assert.Zero(t, s.calls.Load())
				assert.Zero(t, tester.calls.Load())
			}
		}
		for _, media := range []string{"", "text/plain", "application/json; broken"} {
			s, tester := httpFake(), &testerFake{}
			w := adminRequest(httpRouter(s, tester), route.method, route.path, route.body, "admin", media)
			assert.Equal(t, 415, w.Code)
			assert.Zero(t, s.calls.Load())
		}
	}
	s := httpFake()
	body := &countingBody{reader: strings.NewReader(strings.Repeat("x", 4*adminBodyLimit))}
	req := httptest.NewRequest("POST", adminPrefix, nil)
	req.Body = body
	req.Header.Set("Authorization", "admin")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	httpRouter(s, &testerFake{}).ServeHTTP(w, req)
	assert.Equal(t, 413, w.Code)
	assert.Equal(t, adminBodyLimit+1, body.read, "consume at most limit+1 before decoding")
	assert.Zero(t, s.calls.Load())
}

func TestAdminHTTPErrorsAndCancellationAreSafe(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(previous)
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{store.ErrNotFound, 404, "gaigate_not_found"}, {errInvalidInput, 400, "gaigate_invalid_request"},
		{provider.ErrNoCandidate, 503, "gaigate_no_candidate"}, {errors.New("raw-private cipher-private hash-private https://private"), 500, "gaigate_internal"},
	} {
		s := httpFake()
		s.err = tc.err
		w := adminRequest(httpRouter(s, &testerFake{}), "GET", "/1", "", "admin", "")
		assert.Equal(t, tc.status, w.Code)
		assert.Contains(t, w.Body.String(), tc.code)
		assert.NotContains(t, w.Body.String(), "private")
		assert.NotEmpty(t, w.Header().Get("X-Request-Id"))
		assert.Contains(t, w.Body.String(), w.Header().Get("X-Request-Id"))
	}
	assert.NotContains(t, logs.String(), "private")
	s := httpFake()
	tester := &testerFake{test: func(ctx context.Context, ch *store.Channel, model string) (ProbeResult, error) {
		assert.False(t, ch.Enabled, "disabled channel can be diagnosed")
		return ProbeResult{Reason: "upstream_error", StatusCode: 503}, nil
	}}
	w := adminRequest(httpRouter(s, tester), "POST", "/1/test", `{"model":"a"}`, "admin", "application/json")
	assert.Equal(t, 200, w.Code)
	assert.Contains(t, w.Body.String(), `"ok":false`)
	w = adminRequest(httpRouter(s, tester), "POST", "/1/test", `{"model":"unmapped"}`, "admin", "application/json")
	assert.Equal(t, 400, w.Code)
	assert.Equal(t, int32(1), tester.calls.Load())
	ctx, cancel := context.WithCancel(context.Background())
	tester.test = func(context.Context, *store.Channel, string) (ProbeResult, error) {
		cancel()
		return ProbeResult{}, context.Canceled
	}
	req := httptest.NewRequest("POST", adminPrefix+"/1/test", strings.NewReader(`{"model":"a"}`)).WithContext(ctx)
	req.Header.Set("Authorization", "admin")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	assert.PanicsWithValue(t, http.ErrAbortHandler, func() { httpRouter(s, tester).ServeHTTP(w, req) })
	assert.Empty(t, w.Body.String(), "parent cancellation must not append a response")
}

func TestAdminHTTPConstructorRejectsNilAndTypedNil(t *testing.T) {
	s, tester := httpFake(), &testerFake{}
	var nilService *httpServiceFake
	var nilTester *testerFake
	auth := func(*gin.Context) {}
	isAdmin := func(context.Context) bool { return true }
	for _, run := range []func(){
		func() { RegisterAdminRoutes(nil, auth, isAdmin, s, tester) }, func() { RegisterAdminRoutes(gin.New(), nil, isAdmin, s, tester) },
		func() { RegisterAdminRoutes(gin.New(), auth, nil, s, tester) }, func() { RegisterAdminRoutes(gin.New(), auth, isAdmin, nilService, tester) },
		func() { RegisterAdminRoutes(gin.New(), auth, isAdmin, s, nilTester) },
	} {
		assert.Panics(t, run)
	}
}
