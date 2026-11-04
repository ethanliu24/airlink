package event

import (
	"fmt"
	"reflect"
)

type Event interface {
	Name() string
	Payload() any
}

type event struct {
	name    string
	payload any
}

func (e *event) Name() string {
	return e.name
}

func (e *event) Payload() any {
	return e.payload
}

func validatePayload(name, payload any) error {
	v := reflect.ValueOf(payload)

	if v.Kind() != reflect.Pointer {
		return fmt.Errorf("payload for event '%s' must be a pointer, got %s", name, v.Kind())
	}

	if v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("payload for event '%s' must point to a struct, got pointer to %s", name, v.Elem().Kind())
	}

	return nil
}

func NewEvent(name string, payload any) (Event, error) {
	err := validatePayload(name, payload)
	if err != nil {
		return nil, err
	}

	return &event{
		name: name,
		payload: payload,
	}, nil
}
