package eventsource

import "testing"

func TestShouldSnapshot(t *testing.T) {
	tests := []struct {
		name       string
		oldVersion int
		newVersion int
		expected   bool
	}{
		{
			name:       "No Snapshot 90 to 99",
			oldVersion: 90,
			newVersion: 99,
			expected:   false,
		},
		{
			name:       "Yes Snapshot 99 to 101",
			oldVersion: 99,
			newVersion: 101,
			expected:   true,
		},
		{
			name:       "Snapshot 90 to 100",
			oldVersion: 90,
			newVersion: 100,
			expected:   true,
		},
		{
			name:       "Snapshot 100 to 101",
			oldVersion: 100,
			newVersion: 101,
			expected:   false,
		},
		{
			name:       "Snapshot 0 to 101",
			oldVersion: 0,
			newVersion: 101,
			expected:   true,
		},
		{
			name:       "Snapshot 199 to 200",
			oldVersion: 199,
			newVersion: 200,
			expected:   true,
		},
		{
			name:       "No Snapshot 150 to 160",
			oldVersion: 150,
			newVersion: 160,
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := ShouldSnapshot(tt.oldVersion, tt.newVersion); result != tt.expected {
				t.Errorf("%s: expected %v, got %v", tt.name, tt.expected, result)
			}
		})
	}
}
