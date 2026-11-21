package status

import "testing"

func TestStatusValues(t *testing.T) {
	if string(Operational) != "operational" {
		t.Fatalf("operational = %q", string(Operational))
	}
	if string(Down) != "down" {
		t.Fatalf("down = %q", string(Down))
	}
	if string(Unknown) != "unknown" {
		t.Fatalf("unknown = %q", string(Unknown))
	}
}

func TestEmoji(t *testing.T) {
	for _, tc := range []struct {
		s    Status
		want string
	}{{Operational, "🟢"}, {Down, "🔴"}, {Unknown, "⚪"}} {
		if got := tc.s.Emoji(); got != tc.want {
			t.Errorf("%s.Emoji() = %q, want %q", tc.s, got, tc.want)
		}
	}
}

func TestNotifiable(t *testing.T) {
	if !Operational.Notifiable() || !Down.Notifiable() {
		t.Fatal("Operational and Down must be notifiable")
	}
	if Unknown.Notifiable() {
		t.Fatal("Unknown must not be notifiable")
	}
}
