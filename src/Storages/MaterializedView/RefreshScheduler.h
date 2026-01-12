#pragma once

#include <Storages/MaterializedView/RefreshSchedule.h>
#include <Storages/MaterializedView/RefreshSettings.h>
#include <Core/Types.h>

#include <algorithm>
#include <chrono>

namespace DB
{

namespace RefreshSetting
{
    extern const RefreshSettingsInt64 refresh_retries;
    extern const RefreshSettingsUInt64 refresh_retry_initial_backoff_ms;
    extern const RefreshSettingsUInt64 refresh_retry_max_backoff_ms;
}

inline std::chrono::milliseconds refreshBackoff(Int64 retry_idx, const RefreshSettings & refresh_settings)
{
    UInt64 delay_ms;
    UInt64 multiplier = UInt64(1) << std::min(retry_idx, Int64(62));
    /// Overflow check: a*b <= c iff a <= c/b iff a <= floor(c/b).
    if (refresh_settings[RefreshSetting::refresh_retry_initial_backoff_ms] <= refresh_settings[RefreshSetting::refresh_retry_max_backoff_ms] / multiplier)
        delay_ms = refresh_settings[RefreshSetting::refresh_retry_initial_backoff_ms] * multiplier;
    else
        delay_ms = refresh_settings[RefreshSetting::refresh_retry_max_backoff_ms];
    return std::chrono::milliseconds(delay_ms);
}

template <typename State>
inline std::tuple<std::chrono::system_clock::time_point, std::chrono::sys_seconds, State>
planNextRefresh(std::chrono::sys_seconds now, const RefreshSchedule & refresh_schedule, const RefreshSettings & refresh_settings, const String & replica_name, State state)
{
    if (refresh_settings[RefreshSetting::refresh_retries] >= 0 && state.attempt_number > refresh_settings[RefreshSetting::refresh_retries])
    {
        /// Skip to the next scheduled refresh, as if a refresh succeeded.
        state.last_completed_timeslot = refresh_schedule.timeslotForCompletedRefresh(state.last_completed_timeslot, state.last_attempt_time, state.last_attempt_time, false);
        state.attempt_number = 0;
    }
    auto timeslot = refresh_schedule.advance(state.last_completed_timeslot);

    std::chrono::system_clock::time_point when;
    if (state.attempt_number == 0)
        when = refresh_schedule.addRandomSpread(timeslot, state.randomness);
    else
        when = state.last_attempt_time + refreshBackoff(state.attempt_number - 1, refresh_settings);

    state.previous_attempt_error = "";
    if (!state.last_attempt_succeeded && state.last_attempt_time.time_since_epoch().count() != 0)
    {
        if (state.last_attempt_error.empty())
            state.previous_attempt_error = "Replica '" + state.last_attempt_replica + "' went away";
        else
            state.previous_attempt_error = state.last_attempt_error;
    }

    state.attempt_number += 1;
    state.last_attempt_time = now;
    state.last_attempt_replica = replica_name;
    state.last_attempt_error = "";
    state.last_attempt_succeeded = false;

    return {when, timeslot, state};
}

}
