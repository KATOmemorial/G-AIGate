package openai

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"testing"
	"time"

	"gaigate/internal/provider"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func richRequest() *provider.ChatRequest {
	return &provider.ChatRequest{
		Model: "public-alias", Stream: true,
		Messages: []json.RawMessage{
			json.RawMessage(`{"role":"user","content":[{"type":"text","text":"hello"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}],"unknown":{"nested":[1,true,null]}}`),
			json.RawMessage(`{"role":"assistant","content":null,"tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"x\":1}"}}]}`),
		},
		Extra: map[string]json.RawMessage{
			"tools":       json.RawMessage(`[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}]`),
			"temperature": json.RawMessage(`0.25`), "unknown": json.RawMessage(`{"array":[1,"x",null]}`),
			"stream_options": json.RawMessage(`{"include_usage":false,"other":{"nested":[2,3]}}`),
			"model":          json.RawMessage(`"must-not-win"`), "messages": json.RawMessage(`[]`), "stream": json.RawMessage(`null`),
		},
	}
}

func TestSerializePreservesFieldsAndConcurrentInput(t *testing.T) {
	req := richRequest()
	ep := &provider.Endpoint{Model: "mapped-model"}
	before, err := json.Marshal(req)
	require.NoError(t, err)
	for _, stream := range []bool{false, true} {
		body, err := serialize(ep, req, stream)
		require.NoError(t, err)
		fields, err := jsonObject(body)
		require.NoError(t, err)
		require.JSONEq(t, `"mapped-model"`, string(fields["model"]))
		require.JSONEq(t, fmt.Sprint(stream), string(fields["stream"]))
		messages, err := json.Marshal(req.Messages)
		require.NoError(t, err)
		require.JSONEq(t, string(messages), string(fields["messages"]))
		for _, name := range []string{"tools", "temperature", "unknown"} {
			require.JSONEq(t, string(req.Extra[name]), string(fields[name]))
		}
		if stream {
			require.JSONEq(t, `{"include_usage":true,"other":{"nested":[2,3]}}`, string(fields["stream_options"]))
		} else {
			require.JSONEq(t, string(req.Extra["stream_options"]), string(fields["stream_options"]))
		}
		var group errgroup.Group
		for range 32 {
			group.Go(func() error {
				actual, err := serialize(ep, req, stream)
				if err == nil && string(actual) != string(body) {
					return fmt.Errorf("concurrent serialization changed")
				}
				return err
			})
		}
		require.NoError(t, group.Wait())
	}
	after, err := json.Marshal(req)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after), "including nested RawMessage bytes")
}

func TestSerializeStreamOptionsAndInvalidInput(t *testing.T) {
	for _, raw := range []string{"", "null", "{}", `{"include_usage":false}`} {
		req := basicRequest()
		if raw != "" {
			req.Extra = map[string]json.RawMessage{"stream_options": json.RawMessage(raw)}
		}
		body, err := serialize(&provider.Endpoint{Model: "m"}, req, true)
		require.NoError(t, err)
		fields, err := jsonObject(body)
		require.NoError(t, err)
		require.JSONEq(t, `{"include_usage":true}`, string(fields["stream_options"]))
		body, err = serialize(&provider.Endpoint{Model: "m"}, req, false)
		require.NoError(t, err)
		fields, err = jsonObject(body)
		require.NoError(t, err)
		if raw == "" {
			require.NotContains(t, fields, "stream_options")
		} else {
			require.JSONEq(t, raw, string(fields["stream_options"]))
		}
	}
	for _, raw := range []string{`[]`, `false`, `"x"`, `{"broken"`} {
		req := basicRequest()
		req.Extra = map[string]json.RawMessage{"stream_options": json.RawMessage(raw)}
		_, err := serialize(&provider.Endpoint{}, req, true)
		require.Error(t, err)
	}
	for _, raw := range []string{`null`, `[]`, `1`, `{"broken"`} {
		req := basicRequest()
		req.Messages[0] = json.RawMessage(raw)
		_, err := serialize(&provider.Endpoint{}, req, false)
		require.Error(t, err)
	}
	_, err := serialize(nil, basicRequest(), false)
	require.Error(t, err)
	_, err = serialize(&provider.Endpoint{}, nil, false)
	require.Error(t, err)
}

func TestNewCopiesClientPolicy(t *testing.T) {
	_, err := New(nil, 64)
	require.Error(t, err)
	for _, capacity := range []int{0, -1} {
		_, err = New(&http.Client{}, capacity)
		require.Error(t, err)
	}
	transport := &http.Transport{MaxIdleConns: 17}
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	u, err := url.Parse("http://upstream.test")
	require.NoError(t, err)
	jar.SetCookies(u, []*http.Cookie{{Name: "private", Value: "cookie"}})
	redirect := func(*http.Request, []*http.Request) error { return nil }
	client := &http.Client{Timeout: time.Nanosecond, Transport: transport, Jar: jar, CheckRedirect: redirect}
	p, err := New(client, 7)
	require.NoError(t, err)
	a := p.(*adapter)
	require.Equal(t, "openai", p.Name())
	require.Equal(t, 7, a.streamBuffer)
	require.Zero(t, a.client.Timeout)
	require.Nil(t, a.client.Jar)
	require.Same(t, transport, a.client.Transport)
	require.Equal(t, time.Nanosecond, client.Timeout)
	require.Same(t, jar, client.Jar)
	require.NoError(t, client.CheckRedirect(nil, nil))
	require.ErrorIs(t, a.client.CheckRedirect(nil, nil), http.ErrUseLastResponse)
	require.Equal(t, 17, transport.MaxIdleConns)
}
