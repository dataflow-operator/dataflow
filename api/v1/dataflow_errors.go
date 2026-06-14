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

package v1

const (
	// ErrorAckPolicyAfterWrite commits source offsets after the error sink successfully writes (default).
	ErrorAckPolicyAfterWrite = "afterWrite"
	// ErrorAckPolicyNever does not commit source offsets for messages sent to the error sink.
	ErrorAckPolicyNever = "never"
	// ErrorAckPolicyAfterMainSinkSuccess commits offsets only after main sink success (same as never on error path).
	ErrorAckPolicyAfterMainSinkSuccess = "afterMainSinkSuccess"
)

// ErrorSinkSpec defines the error sink and how source ack behaves for failed messages.
type ErrorSinkSpec struct {
	SinkSpec `json:",inline"`

	// AckPolicy controls when source offsets/checkpoints are committed for messages routed to the error sink.
	// afterWrite (default): ack after error sink write.
	// never: do not ack failed messages (they may be re-read on restart).
	// afterMainSinkSuccess: ack only after main sink success (failed messages are not acked).
	// +kubebuilder:validation:Enum=afterWrite;never;afterMainSinkSuccess
	// +kubebuilder:default:=afterWrite
	// +optional
	AckPolicy string `json:"ackPolicy,omitempty"`
}

// ErrorAckPolicyOrDefault returns the configured error sink ack policy.
func ErrorAckPolicyOrDefault(errors *ErrorSinkSpec) string {
	if errors == nil || errors.AckPolicy == "" {
		return ErrorAckPolicyAfterWrite
	}
	switch errors.AckPolicy {
	case ErrorAckPolicyNever, ErrorAckPolicyAfterMainSinkSuccess:
		return errors.AckPolicy
	default:
		return ErrorAckPolicyAfterWrite
	}
}

// ShouldAckOnErrorSink reports whether source ack should propagate when writing to the error sink.
func ShouldAckOnErrorSink(errors *ErrorSinkSpec) bool {
	switch ErrorAckPolicyOrDefault(errors) {
	case ErrorAckPolicyNever, ErrorAckPolicyAfterMainSinkSuccess:
		return false
	default:
		return true
	}
}
