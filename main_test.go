package main

import (
	"strings"
	"testing"
)

func TestTrimForDisplay(t *testing.T) {
	entries := &entries{
		Entries: []entry{
			{},
			{},
			{},
		},
	}
	_, err := entries.TrimForDisplay(1, 2, tile{start: 10, end: 20, size: 10, logURL: "http://example.com"})
	if err == nil {
		t.Fatal("expected error, got none")
	}
	if !strings.Contains(err.Error(), "internal inconsistency") {
		t.Errorf("expected internal inconsistency error, got %s", err)
	}

	_, err = entries.TrimForDisplay(999, 1000, tile{start: 10, end: 20, size: 10, logURL: "http://example.com"})
	if err == nil {
		t.Fatal("expected error, got none")
	}
	if !strings.Contains(err.Error(), "internal inconsistency") {
		t.Errorf("expected internal inconsistency error, got %s", err)
	}

	_, err = entries.TrimForDisplay(1000, 1000, tile{start: 10, end: 20, size: 10, logURL: "http://example.com"})
	if err == nil {
		t.Fatal("expected error, got none")
	}
	if !strings.Contains(err.Error(), "internal inconsistency") {
		t.Errorf("expected internal inconsistency error, got %s", err)
	}

	_, err = entries.TrimForDisplay(10, 20, tile{start: 10, end: 12, size: 2, logURL: "http://example.com"})
	if err == nil {
		t.Fatal("expected error, got none")
	}
	if !strings.Contains(err.Error(), "internal inconsistency") {
		t.Errorf("expected internal inconsistency error, got %s", err)
	}

	_, err = entries.TrimForDisplay(15, 20, tile{start: 10, end: 20, size: 10, logURL: "http://example.com"})
	if err == nil {
		t.Fatal("expected error, got none")
	}
	if !strings.Contains(err.Error(), "past the end of the log") {
		t.Errorf("expected 'past the end of the log' error, got %s", err)
	}

	e, err := entries.TrimForDisplay(10, 20, tile{start: 10, end: 20, size: 10, logURL: "http://example.com"})
	if err != nil {
		t.Fatalf("expected success, got %s", err)
	}
	if len(e.Entries) != 3 {
		t.Errorf("expected 3 entries got %d", len(entries.Entries))
	}

	e, err = entries.TrimForDisplay(11, 12, tile{start: 10, end: 20, size: 10, logURL: "http://example.com"})
	if err != nil {
		t.Fatalf("expected success, got %s", err)
	}
	if len(e.Entries) != 1 {
		t.Errorf("expected 1 entry got %d", len(entries.Entries))
	}

	e, err = entries.TrimForDisplay(12, 20, tile{start: 10, end: 20, size: 10, logURL: "http://example.com"})
	if err != nil {
		t.Fatalf("expected success, got %s", err)
	}
	if len(e.Entries) != 1 {
		t.Errorf("expected 1 entry got %d", len(entries.Entries))
	}
}
