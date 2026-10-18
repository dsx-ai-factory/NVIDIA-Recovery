// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"slices"
	"strconv"
	"strings"
	"testing"
)

type hostlistCase struct {
	input string
	want  []string
}

func TestExpandNodeListMatchesValidatedWFCMatrix(t *testing.T) {
	t.Parallel()

	tests := []hostlistCase{
		{input: "n01p[001-005]", want: []string{"n01p001", "n01p002", "n01p003", "n01p004", "n01p005"}},
		{input: "ap[1-3]z", want: []string{"ap1z", "ap2z", "ap3z"}},
		{input: "dgx[01-03]", want: []string{"dgx01", "dgx02", "dgx03"}},
		{input: "adev[06,13,15]", want: []string{"adev06", "adev13", "adev15"}},
		{input: "lx[062-064,128]", want: []string{"lx062", "lx063", "lx064", "lx128"}},
		{input: "s02p[017,029-031]", want: []string{"s02p017", "s02p029", "s02p030", "s02p031"}},
		{input: "adev[009-101]", want: prefixedRange("adev", 9, 101, 3, "")},
		{input: "s02p[044,046,049]", want: []string{"s02p044", "s02p046", "s02p049"}},
		{input: "nodewithoutnumber", want: []string{"nodewithoutnumber"}},
		{input: "n04p[036,043-044,046-047]", want: []string{"n04p036", "n04p043", "n04p044", "n04p046", "n04p047"}},
		{
			input: "n04p[036,043-044,046-047],nodewithoutnumber,s02p[017,029-031]",
			want:  []string{"n04p036", "n04p043", "n04p044", "n04p046", "n04p047", "nodewithoutnumber", "s02p017", "s02p029", "s02p030", "s02p031"},
		},
		{
			input: "n04p[036,043-044,046-047], nodewithoutnumber, s02p[017,029-031]",
			want:  []string{"n04p036", "n04p043", "n04p044", "n04p046", "n04p047", "nodewithoutnumber", "s02p017", "s02p029", "s02p030", "s02p031"},
		},
		{
			input: "n04p[036,043-044,046-047], s02p[017,029-031]",
			want:  []string{"n04p036", "n04p043", "n04p044", "n04p046", "n04p047", "s02p017", "s02p029", "s02p030", "s02p031"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := ExpandNodeList(tt.input)
			if err != nil {
				t.Fatalf("ExpandNodeList(%q) failed: %v", tt.input, err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("ExpandNodeList(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}

func TestExpandNodeListMatchesPythonWFCMatrix(t *testing.T) {
	t.Parallel()

	tests := []hostlistCase{
		{input: "", want: nil},
		{input: "example.hostname.com", want: []string{"example.hostname.com"}},
		{input: "example.com", want: []string{"example.com"}},
		{input: "example", want: []string{"example"}},
		{input: "example[2-4].hostname.com", want: []string{"example2.hostname.com", "example3.hostname.com", "example4.hostname.com"}},
		{input: "example[02-4].hostname.com", want: []string{"example02.hostname.com", "example03.hostname.com", "example04.hostname.com"}},
		{input: "example[2-04].hostname.com", want: []string{"example02.hostname.com", "example03.hostname.com", "example04.hostname.com"}},
		{input: "example[02-04].hostname.com", want: []string{"example02.hostname.com", "example03.hostname.com", "example04.hostname.com"}},
		{input: "example[2,10,20].hostname.com", want: []string{"example2.hostname.com", "example10.hostname.com", "example20.hostname.com"}},
		{input: "example[002,10,20].hostname.com", want: []string{"example002.hostname.com", "example10.hostname.com", "example20.hostname.com"}},
		{input: "example[2-4,10,20-21].hostname.com", want: []string{"example2.hostname.com", "example3.hostname.com", "example4.hostname.com", "example10.hostname.com", "example20.hostname.com", "example21.hostname.com"}},
		{input: "example[2-4].hostname[30-31]", want: []string{"example2.hostname30", "example2.hostname31", "example3.hostname30", "example3.hostname31", "example4.hostname30", "example4.hostname31"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := ExpandNodeList(tt.input)
			if err != nil {
				t.Fatalf("ExpandNodeList(%q) failed: %v", tt.input, err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("ExpandNodeList(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}

func TestExpandNodeListControllerRegressions(t *testing.T) {
	t.Parallel()

	tests := []hostlistCase{
		{input: "gpu-node-[001-002]", want: []string{"gpu-node-001", "gpu-node-002"}},
		{input: "rack[1-2]node[01-02]", want: []string{"rack1node01", "rack1node02", "rack2node01", "rack2node02"}},
		{input: "gpu-node-[001-002], gpu-node-010", want: []string{"gpu-node-001", "gpu-node-002", "gpu-node-010"}},
		{input: "None assigned", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			got, err := ExpandNodeList(tt.input)
			if err != nil {
				t.Fatalf("ExpandNodeList(%q) failed: %v", tt.input, err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("ExpandNodeList(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}

func TestExpandNodeListRejectsMalformedExpressions(t *testing.T) {
	t.Parallel()

	tests := []string{
		"node[1-]",
		"node[a]",
		"node[]",
		"node[1,,2]",
		"node[1-2-3]",
		"node[2-1]",
		"node[1-2",
		"node1-2]",
		"node[[1-2]]",
		"a[1]b[2]c[3]",
		"node[18446744073709551616]",
		"node1,node[1-]",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			t.Parallel()

			got, err := ExpandNodeList(input)
			if err == nil {
				t.Fatalf("ExpandNodeList(%q) error = nil, want malformed-expression error", input)
			}
			if got != nil {
				t.Fatalf("ExpandNodeList(%q) = %#v, want nil on error", input, got)
			}
		})
	}
}

func TestExpandNodeListExpandsEqualRange(t *testing.T) {
	t.Parallel()

	got, err := ExpandNodeList("node[2-2]")
	if err != nil {
		t.Fatalf("ExpandNodeList(%q) failed: %v", "node[2-2]", err)
	}
	want := []string{"node2"}
	if !slices.Equal(got, want) {
		t.Fatalf("ExpandNodeList(%q) = %#v, want %#v", "node[2-2]", got, want)
	}
}

func TestExpandNodeListPreservesOrderAndDuplicates(t *testing.T) {
	t.Parallel()

	got, err := ExpandNodeList("node[3,1,2-3],node1")
	if err != nil {
		t.Fatalf("ExpandNodeList() failed: %v", err)
	}
	want := []string{"node3", "node1", "node2", "node3", "node1"}
	if !slices.Equal(got, want) {
		t.Fatalf("ExpandNodeList() = %#v, want %#v", got, want)
	}
}

func TestExpandNodeListErrorDoesNotEchoUnboundedInput(t *testing.T) {
	t.Parallel()

	numericPayload := strings.Repeat("9", 1024)
	_, err := ExpandNodeList("node[" + numericPayload + "]")
	if err == nil {
		t.Fatal("ExpandNodeList() error = nil, want numeric overflow error")
	}
	if strings.Contains(err.Error(), numericPayload) {
		t.Fatal("ExpandNodeList() error contains the unbounded scheduler payload")
	}
}

func TestExpandCPUListMatchesValidatedWFCMatrix(t *testing.T) {
	t.Parallel()

	tests := []hostlistCase{
		{input: "36-45,84-85", want: []string{"36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "84", "85"}},
		{input: "098-102,199", want: []string{"98", "99", "100", "101", "102", "199"}},
		{input: "002-005", want: []string{"2", "3", "4", "5"}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			if got := ExpandCPUList(tt.input); !slices.Equal(got, tt.want) {
				t.Fatalf("ExpandCPUList(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}

func prefixedRange(prefix string, minimum, maximum, width int, suffix string) []string {
	out := make([]string, 0, maximum-minimum+1)
	for value := minimum; value <= maximum; value++ {
		out = append(out, prefix+zeroPad(value, width)+suffix)
	}
	return out
}

func zeroPad(value, width int) string {
	const digits = "0000000000"
	rendered := strconv.Itoa(value)
	if len(rendered) >= width {
		return rendered
	}
	return digits[:width-len(rendered)] + rendered
}
