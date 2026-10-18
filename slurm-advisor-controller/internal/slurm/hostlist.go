// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

func makeRange(minimum, maximum int) []int {
	values := make([]int, maximum-minimum+1)
	for i := range values {
		values[i] = minimum + i
	}
	return values
}

func splitHostExpressions(nodeList string) ([]string, error) {
	var expressions []string
	var current strings.Builder
	squareDepth := 0

	for _, char := range nodeList {
		switch char {
		case '[':
			if squareDepth != 0 {
				return nil, errors.New("nested bracket in host expression")
			}
			squareDepth++
			current.WriteRune(char)
		case ']':
			if squareDepth == 0 {
				return nil, errors.New("unmatched closing bracket in host expression")
			}
			squareDepth--
			current.WriteRune(char)
		case ',':
			if squareDepth == 0 {
				expression := strings.TrimSpace(current.String())
				if expression == "" {
					return nil, errors.New("empty host expression")
				}
				expressions = append(expressions, expression)
				current.Reset()
			} else {
				current.WriteRune(char)
			}
		default:
			current.WriteRune(char)
		}
	}

	if squareDepth != 0 {
		return nil, errors.New("unmatched opening bracket in host expression")
	}
	expression := strings.TrimSpace(current.String())
	if expression == "" {
		return nil, errors.New("empty host expression")
	}
	expressions = append(expressions, expression)

	return expressions, nil
}

func ExpandNodeList(nodeList string) ([]string, error) {
	if strings.TrimSpace(nodeList) == "" || nodeList == "None assigned" {
		return nil, nil
	}

	expressions, err := splitHostExpressions(nodeList)
	if err != nil {
		return nil, err
	}

	var hosts []string
	for index, expression := range expressions {
		expanded, err := expandHostExpression(expression)
		if err != nil {
			return nil, errors.Wrapf(err, "expand host expression %d", index+1)
		}
		if len(expanded) > maximumInt()-len(hosts) {
			return nil, errors.New("host list expansion is too large")
		}
		hosts = append(hosts, expanded...)
	}

	return hosts, nil
}

func expandHostExpression(expression string) ([]string, error) {
	hosts := []string{""}
	literalStart := 0
	groupCount := 0

	for index := 0; index < len(expression); index++ {
		if expression[index] != '[' {
			continue
		}

		groupCount++
		if groupCount > 2 {
			return nil, errors.New("host expression contains more than two bracket groups")
		}

		closingOffset := strings.IndexByte(expression[index+1:], ']')
		if closingOffset < 0 {
			return nil, errors.New("unmatched opening bracket in host expression")
		}
		closingIndex := index + 1 + closingOffset
		values, err := expandNumericExpression(expression[index+1 : closingIndex])
		if err != nil {
			return nil, errors.Wrapf(err, "expand bracket group %d", groupCount)
		}

		if len(hosts) != 0 && len(values) > maximumInt()/len(hosts) {
			return nil, errors.New("host expression Cartesian product is too large")
		}
		literal := expression[literalStart:index]
		next := make([]string, 0, len(hosts)*len(values))
		for _, host := range hosts {
			prefix := host + literal
			for _, value := range values {
				next = append(next, prefix+value)
			}
		}
		hosts = next
		index = closingIndex
		literalStart = closingIndex + 1
	}

	suffix := expression[literalStart:]
	for index := range hosts {
		hosts[index] += suffix
	}
	return hosts, nil
}

func expandNumericExpression(expression string) ([]string, error) {
	if expression == "" {
		return nil, errors.New("empty numeric expression")
	}

	members := strings.Split(expression, ",")
	values := make([]string, 0, len(members))
	for memberIndex, member := range members {
		if member == "" {
			return nil, errors.Newf("numeric member %d is empty", memberIndex+1)
		}

		switch strings.Count(member, "-") {
		case 0:
			if _, err := parseHostNumber(member); err != nil {
				return nil, errors.Wrapf(err, "parse numeric member %d", memberIndex+1)
			}
			values = append(values, member)
		case 1:
			bounds := strings.SplitN(member, "-", 2)
			if bounds[0] == "" || bounds[1] == "" {
				return nil, errors.Newf("numeric member %d has an empty range bound", memberIndex+1)
			}
			minimum, err := parseHostNumber(bounds[0])
			if err != nil {
				return nil, errors.Wrapf(err, "parse numeric member %d lower bound", memberIndex+1)
			}
			maximum, err := parseHostNumber(bounds[1])
			if err != nil {
				return nil, errors.Wrapf(err, "parse numeric member %d upper bound", memberIndex+1)
			}
			if minimum > maximum {
				return nil, errors.Newf("numeric member %d has a descending range", memberIndex+1)
			}

			rangeLength := maximum - minimum + 1
			if rangeLength > uint64(maximumInt()-len(values)) {
				return nil, errors.New("numeric range expansion is too large")
			}
			needed := len(values) + int(rangeLength)
			expanded := make([]string, len(values), needed)
			copy(expanded, values)
			values = expanded

			width := paddedWidth(bounds[0])
			if upperWidth := paddedWidth(bounds[1]); upperWidth > width {
				width = upperWidth
			}
			for value := minimum; ; value++ {
				values = append(values, renderHostNumber(value, width))
				if value == maximum {
					break
				}
			}
		default:
			return nil, errors.Newf("numeric member %d has too many range separators", memberIndex+1)
		}
	}

	return values, nil
}

func parseHostNumber(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, errors.New("decimal value exceeds platform integer range")
		}
		return 0, errors.New("invalid decimal value")
	}
	if parsed > uint64(maximumInt()) {
		return 0, errors.New("decimal value exceeds platform integer range")
	}
	return parsed, nil
}

func paddedWidth(value string) int {
	if len(value) > 1 && value[0] == '0' {
		return len(value)
	}
	return 0
}

func renderHostNumber(value uint64, width int) string {
	rendered := strconv.FormatUint(value, 10)
	if len(rendered) >= width {
		return rendered
	}
	return strings.Repeat("0", width-len(rendered)) + rendered
}

func maximumInt() int {
	return int(^uint(0) >> 1)
}

func ExpandCPUList(cpuList string) []string {
	if strings.TrimSpace(cpuList) == "" {
		return nil
	}

	var cpus []int
	for _, item := range strings.Split(cpuList, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if strings.Contains(item, "-") {
			bounds := strings.SplitN(item, "-", 2)
			first, err := strconv.ParseInt(bounds[0], 10, 0)
			if err != nil {
				return []string{}
			}
			last, err := strconv.ParseInt(bounds[1], 10, 0)
			if err != nil {
				return []string{}
			}
			cpus = append(cpus, makeRange(int(first), int(last))...)
			continue
		}

		parsed, err := strconv.ParseInt(item, 10, 0)
		if err != nil {
			return []string{}
		}
		cpus = append(cpus, int(parsed))
	}
	sort.Ints(cpus)

	out := make([]string, 0, len(cpus))
	for _, cpu := range cpus {
		out = append(out, strconv.Itoa(cpu))
	}
	return out
}
