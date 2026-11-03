package transfer

import (
    "errors"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestManagerRegister(t *testing.T) {
    t.Parallel()

    tests := []struct {
        name          string
        existingIDs   []string
        generatedIDs  []string
        generateErr   error
        expectedID    string
        expectedCalls int
        expectedErr   error
    }{
        {
            name:          "successful registration",
            generatedIDs:  []string{"new-id"},
            expectedID:    "new-id",
            expectedCalls: 1,
        },
        {
            name:          "retry after ID collision",
            existingIDs:   []string{"existing-id"},
            generatedIDs:  []string{"existing-id", "new-id"},
            expectedID:    "new-id",
            expectedCalls: 2,
        },
        {
            name:          "ID generation error",
            generateErr:   errors.New("ID generation failed"),
            expectedCalls: 1,
            expectedErr:   errors.New("ID generation failed"),
        },
        {
            name:          "exhausted retries",
            existingIDs:   []string{"existing-id"},
            generatedIDs:  []string{
                "existing-id",
                "existing-id",
                "existing-id",
            },
            expectedCalls: idGenRetryTimes,
            expectedErr:   errors.New("manager receive transfer id generation max retries reached - impossible!"),
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel()

            var transferUnderTest transfer

            existing := make(map[string]transfer)
            for _, id := range tt.existingIDs {
                existing[id] = nil
            }

            calls := 0

            manager := &Manager{
                transfers: existing,
                generateID: func() (string, error) {
                    calls++

                    if tt.generateErr != nil {
                        return "", tt.generateErr
                    }

                    if calls > len(tt.generatedIDs) {
                        return "", errors.New("unexpected ID generation call")
                    }

                    return tt.generatedIDs[calls-1], nil
                },
            }

            id, err := manager.register(transferUnderTest)

            assert.Equal(t, tt.expectedCalls, calls)

            if tt.expectedErr != nil {
                require.Error(t, err)
                assert.Empty(t, id)

                if tt.name == "ID generation error" {
                    assert.ErrorIs(t, err, tt.generateErr)
                } else {
                    assert.Contains(t, err.Error(), tt.expectedErr.Error())
                }

                return
            }

            require.NoError(t, err)
            assert.Equal(t, tt.expectedID, id)

            manager.mu.Lock()
            registeredTransfer, exists := manager.transfers[id]
            manager.mu.Unlock()

            assert.True(t, exists)
            assert.Equal(t, transferUnderTest, registeredTransfer)
        })
    }
}
