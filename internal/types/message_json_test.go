/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package types

import (
	"testing"
)

func TestMessage_JSONValueCache(t *testing.T) {
	msg := NewMessage([]byte(`{"a":1,"b":"x"}`))

	v1, err := msg.JSONValue()
	if err != nil {
		t.Fatalf("JSONValue: %v", err)
	}
	obj1, ok := v1.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map, got %T", v1)
	}
	if obj1["a"].(float64) != 1 {
		t.Fatalf("unexpected a: %v", obj1["a"])
	}

	// Mutate cached object; second call must return the same cached value.
	obj1["a"] = float64(99)
	v2, err := msg.JSONValue()
	if err != nil {
		t.Fatalf("JSONValue second: %v", err)
	}
	obj2 := v2.(map[string]interface{})
	if obj2["a"].(float64) != 99 {
		t.Fatalf("cache not reused: got a=%v", obj2["a"])
	}

	msg.SetData([]byte(`{"a":2}`))
	v3, err := msg.JSONValue()
	if err != nil {
		t.Fatalf("JSONValue after SetData: %v", err)
	}
	obj3 := v3.(map[string]interface{})
	if obj3["a"].(float64) != 2 {
		t.Fatalf("cache not invalidated: got a=%v", obj3["a"])
	}
}

func TestMessage_JSONObject(t *testing.T) {
	msg := NewMessage([]byte(`[1,2]`))
	if _, ok := msg.JSONObject(); ok {
		t.Fatal("array should not be JSONObject")
	}
	msg = NewMessage([]byte(`{"ok":true}`))
	obj, ok := msg.JSONObject()
	if !ok || obj["ok"] != true {
		t.Fatalf("JSONObject = %v ok=%v", obj, ok)
	}
}

func TestFromJSON_PrimesCache(t *testing.T) {
	src := map[string]interface{}{"id": float64(1)}
	msg, err := FromJSON(src)
	if err != nil {
		t.Fatal(err)
	}
	if !msg.HasCachedJSON() {
		t.Fatal("FromJSON should prime cache")
	}
	got, ok := msg.JSONObject()
	if !ok {
		t.Fatal("expected object")
	}
	if got["id"] != float64(1) {
		t.Fatalf("got %#v", got)
	}
}

func TestMessage_ToJSON_Invalid(t *testing.T) {
	msg := NewMessage([]byte(`not-json`))
	if _, err := msg.ToJSON(); err == nil {
		t.Fatal("expected error")
	}
}
