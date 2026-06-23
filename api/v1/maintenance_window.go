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

import (
	"fmt"
	"time"
)

// +kubebuilder:object:generate=false

// MaintenanceWindowResult is the outcome of evaluating a maintenance schedule.
type MaintenanceWindowResult struct {
	InWindow           bool
	NextWindowStart    time.Time
	CurrentWindowStart time.Time
}

// IsMaintenanceSuspended returns true when spec.maintenance.suspended is explicitly true.
func IsMaintenanceSuspended(spec *MaintenanceSpec) bool {
	return spec != nil && spec.Suspended != nil && *spec.Suspended
}

// IsProcessorPaused returns true when the processor should be scaled to zero.
func IsProcessorPaused(spec *DataFlowSpec, now time.Time) (bool, error) {
	if spec == nil || spec.Maintenance == nil {
		return false, nil
	}
	if IsMaintenanceSuspended(spec.Maintenance) {
		return true, nil
	}
	if spec.Maintenance.StartTime == "" || spec.Maintenance.Duration == "" {
		return false, nil
	}
	result, err := EvaluateMaintenanceWindow(spec.Maintenance, now)
	if err != nil {
		return false, err
	}
	return result.InWindow, nil
}

// EvaluateMaintenanceWindow determines whether now falls inside a maintenance window.
func EvaluateMaintenanceWindow(spec *MaintenanceSpec, now time.Time) (MaintenanceWindowResult, error) {
	var empty MaintenanceWindowResult
	if spec == nil || spec.StartTime == "" || spec.Duration == "" {
		return empty, nil
	}

	startTime, err := time.Parse(time.RFC3339, spec.StartTime)
	if err != nil {
		return empty, fmt.Errorf("invalid startTime: %w", err)
	}

	duration, err := time.ParseDuration(spec.Duration)
	if err != nil {
		return empty, fmt.Errorf("invalid duration: %w", err)
	}
	if duration <= 0 {
		return empty, fmt.Errorf("duration must be greater than zero")
	}

	loc := time.UTC
	if spec.Timezone != "" {
		loc, err = time.LoadLocation(spec.Timezone)
		if err != nil {
			return empty, fmt.Errorf("invalid timezone: %w", err)
		}
	}

	now = now.In(loc)
	anchor := startTime.In(loc)

	switch spec.Repeat {
	case MaintenanceRepeatDaily:
		return evaluateDailyWindow(anchor, duration, now), nil
	case MaintenanceRepeatWeekly:
		return evaluateWeeklyWindow(anchor, duration, now), nil
	case MaintenanceRepeatMonthly:
		return evaluateMonthlyWindow(anchor, duration, now), nil
	default:
		return evaluateOneTimeWindow(anchor, duration, now), nil
	}
}

func evaluateOneTimeWindow(start time.Time, duration time.Duration, now time.Time) MaintenanceWindowResult {
	end := start.Add(duration)
	if now.Before(start) {
		return MaintenanceWindowResult{NextWindowStart: start}
	}
	if !now.Before(end) {
		return MaintenanceWindowResult{}
	}
	return MaintenanceWindowResult{
		InWindow:           true,
		CurrentWindowStart: start,
	}
}

func evaluateDailyWindow(anchor time.Time, duration time.Duration, now time.Time) MaintenanceWindowResult {
	windowStart := time.Date(now.Year(), now.Month(), now.Day(), anchor.Hour(), anchor.Minute(), anchor.Second(), anchor.Nanosecond(), anchor.Location())
	if now.Before(windowStart) {
		prev := windowStart.Add(-24 * time.Hour)
		if inWindow(now, prev, duration) {
			return MaintenanceWindowResult{
				InWindow:           true,
				CurrentWindowStart: prev,
				NextWindowStart:    windowStart,
			}
		}
		return MaintenanceWindowResult{NextWindowStart: windowStart}
	}
	if inWindow(now, windowStart, duration) {
		return MaintenanceWindowResult{
			InWindow:           true,
			CurrentWindowStart: windowStart,
			NextWindowStart:    windowStart.Add(24 * time.Hour),
		}
	}
	return MaintenanceWindowResult{NextWindowStart: windowStart.Add(24 * time.Hour)}
}

func evaluateWeeklyWindow(anchor time.Time, duration time.Duration, now time.Time) MaintenanceWindowResult {
	if now.Before(anchor) {
		return MaintenanceWindowResult{NextWindowStart: anchor}
	}

	elapsed := now.Sub(anchor)
	week := 7 * 24 * time.Hour
	cycle := elapsed / week
	windowStart := anchor.Add(cycle * week)
	if inWindow(now, windowStart, duration) {
		return MaintenanceWindowResult{
			InWindow:           true,
			CurrentWindowStart: windowStart,
			NextWindowStart:    windowStart.Add(week),
		}
	}
	if now.Before(windowStart) {
		return MaintenanceWindowResult{NextWindowStart: windowStart}
	}
	return MaintenanceWindowResult{NextWindowStart: windowStart.Add(week)}
}

func evaluateMonthlyWindow(anchor time.Time, duration time.Duration, now time.Time) MaintenanceWindowResult {
	loc := anchor.Location()
	year, month := now.Year(), now.Month()
	windowStart := monthlyWindowStart(year, month, anchor, loc)

	if now.Before(windowStart) {
		prevYear, prevMonth := year, month-1
		if prevMonth < 1 {
			prevMonth = 12
			prevYear--
		}
		prevStart := monthlyWindowStart(prevYear, prevMonth, anchor, loc)
		if inWindow(now, prevStart, duration) {
			return MaintenanceWindowResult{
				InWindow:           true,
				CurrentWindowStart: prevStart,
				NextWindowStart:    windowStart,
			}
		}
		return MaintenanceWindowResult{NextWindowStart: windowStart}
	}

	if inWindow(now, windowStart, duration) {
		nextYear, nextMonth := year, month+1
		if nextMonth > 12 {
			nextMonth = 1
			nextYear++
		}
		return MaintenanceWindowResult{
			InWindow:           true,
			CurrentWindowStart: windowStart,
			NextWindowStart:    monthlyWindowStart(nextYear, nextMonth, anchor, loc),
		}
	}

	nextYear, nextMonth := year, month+1
	if nextMonth > 12 {
		nextMonth = 1
		nextYear++
	}
	return MaintenanceWindowResult{NextWindowStart: monthlyWindowStart(nextYear, nextMonth, anchor, loc)}
}

func monthlyWindowStart(year int, month time.Month, anchor time.Time, loc *time.Location) time.Time {
	day := anchor.Day()
	lastDay := daysInMonth(year, month)
	if day > lastDay {
		day = lastDay
	}
	return time.Date(year, month, day, anchor.Hour(), anchor.Minute(), anchor.Second(), anchor.Nanosecond(), loc)
}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func inWindow(now, start time.Time, duration time.Duration) bool {
	end := start.Add(duration)
	return !now.Before(start) && now.Before(end)
}
