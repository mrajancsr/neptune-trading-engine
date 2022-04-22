import pandas as pd
import numpy as np
from typing import Any, Dict
import uuid
import os
from post_to_discord import post_to_discord
from exchange_feeds.constants import OPEN, HIGH, LOW, CLOSE, TIME, SYMBOL

MINUTES_IN_DAY = 1440


class Momentum:
    def __init__(
        self,
        token_pair,
        slow_atr=60,
        fast_atr=30,
        roll_period="1440T",
        frequencies=[],
        enter_frequencies=[],
        exit_frequency=None,
        slowest_moving_avg=15,
        backtest=True,
        zscore=2.33,
        sema=None,
        discord_channel=None,
    ) -> None:
        self._token = token_pair
        self._dataframe = pd.DataFrame()
        self._frequencies = [float(x) for x in frequencies]  # array of tuples
        self._slow_atr = int(slow_atr)  # in periods (minutes in default case)
        self._fast_atr = int(fast_atr)  # in periods (minutes in default case)
        self._roll_period = roll_period  # In time
        self._exit_frequency = float(exit_frequency)
        self._slowest_moving_avg = float(slowest_moving_avg)
        self._max_rows = MINUTES_IN_DAY * self._slowest_moving_avg
        self._backtest = backtest
        self._zscore = float(zscore)
        self._uuid = uuid.uuid4()
        self._discord_channel = discord_channel
        if sema is not None:
            sema.release()

    def process_signal(self):
        self._dataframe[SYMBOL] = self._token
        self._dataframe[TIME] = pd.to_datetime(self._dataframe[TIME], unit="s")
        self._dataframe.index.name = None
        if self._backtest:
            self._dataframe.sort_values(TIME, inplace=True)
            self._dataframe.set_index(TIME, inplace=True)
        self._dataframe.sort_index(inplace=True)
        self._dataframe["avg_price"] = (
            self._dataframe[HIGH] + self._dataframe[LOW] + self._dataframe[CLOSE]
        ) / 3
        self._dataframe["return"] = np.log(self._dataframe.avg_price) - np.log(
            self._dataframe.avg_price.shift(1)
        )
        self._dataframe["slow_atr"] = Momentum.compute_average_true_range(
            self, self._dataframe, self._slow_atr
        )
        self._dataframe["atr_1d"] = (
            self._dataframe["slow_atr"].rolling(self._roll_period).mean()
        )
        self._dataframe["atr_vol_periods_vector"] = Momentum.compute_average_true_range(
            self, self._dataframe, self._fast_atr
        )
        self._dataframe["atr_vol"] = (
            self._dataframe["atr_vol_periods_vector"].rolling(self._roll_period).std()
        )
        self._dataframe["atr_range"] = self._dataframe["atr_1d"].shift(
            1
        ) + self._zscore * self._dataframe["atr_vol"].shift(1)
        self._dataframe["vol_breakout"] = np.where(
            self._dataframe["atr_range"] > self._dataframe["slow_atr"].shift(1), 0, 1
        )
        Momentum.MAs(self)
        result, entries, exits, short_entries, short_exits = Momentum.calculate_signal(
            self,
            total_signal="total_signal",
            exit_level=f"signal{self._exit_frequency}D",
            slowest_ma=f"signal{self._slowest_moving_avg}D",
            vol_breakout="vol_breakout",
        )
        actual_signal = result
        newest_signal = self._dataframe.iloc[-1]
        formatted_new_signal = f"{self._token} Open: {newest_signal[OPEN]} High: {newest_signal[HIGH]} Low: {newest_signal[LOW]} Close: {newest_signal[CLOSE]}"
        post_to_discord(self._discord_channel, formatted_new_signal)

        # Only run over all of history and output for vectorbt if we're backtesting
        if self._backtest:
            self._dataframe["entries"] = entries
            self._dataframe["exits"] = exits
            self._dataframe["short_entries"] = short_entries
            self._dataframe["short_exits"] = short_exits
            calculations = Momentum.calculations(
                self, trade_signal=actual_signal, df=self._dataframe, coin=self._token
            )
            calculations.to_csv(
                f"{os.get_cwd()}/signal_results/{self._token}{self._uuid}.csv"
            )

    def load_flatfile(self, path_to_ff, skiprows=1, numrows=None):
        # self._dataframe = pd.read_csv(path_to_ff, skiprows=skiprows, nrows=numrows, names=['time', 'to', 'from', 'open',
        #                                                                       'high', 'low', 'close', 'volume',
        #                                                                       'open_interest'],
        #                               parse_dates=['time']) ## CCAGG data - leaving for now until our data picture stabilizes
        self._dataframe = self._dataframe = pd.read_csv(
            path_to_ff,
            skiprows=skiprows,
            nrows=numrows,
            names=[TIME, OPEN, HIGH, LOW, CLOSE, "volume", "open_interest"],
            parse_dates=[TIME],
        )
        self.process_signal()

    def append_bar(self, bar, empty_bar=False):
        self._dataframe = pd.concat(
            [self._dataframe, bar]
        )  # TODO fail if appending an identical minute
        if empty_bar:
            """
            We still want evenly spaced minute bars for calculating MAs, so just take the previous bars values
            and stuff it into this bar
            """
            self._dataframe = self._dataframe.ffill()
        else:
            self.process_signal()

    def load_from_database(self):
        # TODO get from DB
        self.process_signal()

    def prune_to_size(self, rows) -> None:
        # Removes rows outside of trading window
        self._dataframe = self._dataframe.tail(rows)

    def signal_update_loop(self, bar):
        self._dataframe = self._dataframe.append(bar)
        self._dataframe = self._dataframe.tail(self._max_rows)

    @classmethod
    def compute_wilder_moving_average(self, values, n) -> pd.Series:
        # input is a dataframe
        return values.ewm(alpha=1 / n, adjust=False).mean()

    def compute_sma(self) -> None:
        # mutates dataframe when new data is appended
        pass

    def set_sma(self, window, value) -> None:
        self._simple_moving_averages["window"] = value

    def compute_average_true_range(self, df, n=14) -> pd.Series:
        self._dataframe["prev_close"] = df[CLOSE].shift()
        self._dataframe["high_low_range"] = abs(df[HIGH] - df[LOW])
        self._dataframe["high_prev_close_range"] = abs(df[HIGH] - df["prev_close"])
        self._dataframe["low_prev_close_range"] = abs(df[LOW] - df["prev_close"])
        tr = self._dataframe[
            ["high_low_range", "high_prev_close_range", "low_prev_close_range"]
        ].max(
            axis=1
        )  # breaks here?
        atr = Momentum.compute_wilder_moving_average(tr, n)
        return atr

    @property
    def simple_moving_averages(self) -> Dict:
        return self._simple_moving_averages

    def MAs(self) -> pd.DataFrame:
        colname = "avg_price"
        freqs_for_tup_conversion = self._frequencies + list([self._exit_frequency])
        frequencies = []
        for freq in freqs_for_tup_conversion:
            frequencies.append((f"{freq}D", f"{freq * MINUTES_IN_DAY}T"))
        enter_frequencies = self._frequencies

        for freq in frequencies:
            outcol, offset_arg = freq
            self._dataframe[f"MA{outcol}"] = np.round(
                Momentum.sma(self._dataframe, offset_arg, colname), 5
            )
            # Meaning when the curent price is GREATER THAN the MA we're testing...
            self._dataframe[f"signal{outcol}"] = np.where(
                self._dataframe[f"MA{outcol}"] <= self._dataframe[colname], 1, -1
            )
            self._dataframe[f"signal{outcol}"] = self._dataframe[
                f"signal{outcol}"
            ].shift(1)

        enter_keys = " ".join(f"signal{x}D" for x in enter_frequencies).split(" ")
        self._dataframe["total_signal"] = 0
        for key in enter_keys:
            self._dataframe["total_signal"] = (
                self._dataframe["total_signal"] + self._dataframe[key]
            )

    @classmethod
    def sma(self, df, period, colname) -> np.float:
        return df.rolling(period)[colname].mean()

    def calculations(self, trade_signal, df, coin) -> pd.DataFrame:

        colname = "avg_price"
        df = df.copy()
        df["signal"] = trade_signal
        df["position_shift"] = df["signal"].diff()  # the days where the signal shifts
        signal_switch_df = df[df["position_shift"] != 0]
        print(f"Length of switch: {len(signal_switch_df)}")
        # TODO: integration with smoothbrain == no more need for file paths
        signal_switch_df.to_csv(
            f"{os.get_cwd()}/signal_results/{self._token}{self._uuid}_where_it_switches.csv"
        )
        with open(
            f"{os.get_cwd()}/signal_results/{self._token}{self._uuid}.txt",
            "w",
        ) as metadata:
            metadata.write(
                f"{self._frequencies} {self._exit_frequency} {self._slow_atr} {self._fast_atr} {self._zscore} {self._roll_period}"
            )
        return df

    def calculate_signal(self, total_signal, exit_level, slowest_ma, vol_breakout):
        """
        a = total_signal
        b = signal_10
        c = exit_level
        d = signal_15
        e = signal_20
        f = vol_breakout

        res = result, with values 1 or -1 for LONG or SHORT
        """

        # Initialize signal vector to start at previously computed total_signal
        total_signal = self._dataframe[total_signal].T
        result = total_signal.copy()
        temp = total_signal.copy()
        exit_level = self._dataframe[exit_level]
        vol_breakout = self._dataframe[vol_breakout]
        slowest_ma = self._dataframe[slowest_ma]
        entries = total_signal.copy()
        exits = total_signal.copy()
        short_entries = total_signal.copy()
        short_exits = total_signal.copy()
        entries[:] = False
        exits[:] = False
        short_entries[:] = False
        short_exits[:] = False

        indices = result.index.tolist()
        counter = -1
        for t in indices:
            # counter is just an integer ref to what row we're on, needed for going back a row
            counter += 1
            prev_result = result[counter - 1]
            minimum_minutes = MINUTES_IN_DAY * self._slowest_moving_avg
            if counter < minimum_minutes:
                temp[t] = 0
                result[t] = 0
                continue
            if total_signal[t] == 3:  # Period is long
                # If last period was not long and we're in a vol breakout, go long
                if temp[counter - 1] != 3 and vol_breakout[t] == 1:
                    result[t] = 1
                    temp[t] = total_signal[t]
                    entries[t] = True

                # If last period was long, stay long
                elif temp[counter - 1] == 3:
                    result[t] = 1
                    temp[t] = total_signal[t]
                # If we're not in vol breakout yet, stay flat
                else:
                    temp[t] = 0
                    result[t] = 0
            elif total_signal[t] == -3:  # If we're short today
                # if last period was not short and we're in a breakout, go short
                if temp[counter - 1] != -3 and vol_breakout[t] == 1:
                    result[t] = -1
                    temp[t] = total_signal[t]
                    short_entries[t] = True
                # if last period was short, stay short
                elif temp[counter - 1] == -3:
                    result[t] = -1
                    temp[t] = total_signal[t]
                else:  # Not in a vol breakout yet, and last period was not short, so no position
                    temp[t] = 0
                    result[t] = 0
            elif exit_level[t] > 0 and total_signal[t] != 3 and slowest_ma[t] > 0:
                """
                If we were previously long and previously above exit level
                AND
                we're currently above exit level and the longest MA, stay long.
                Otherwise sell out.
                """

                if (
                    temp[counter - 1] == 3
                    and exit_level[t] == 1
                    and exit_level[counter - 1] == 1
                    and slowest_ma[t] == 1
                ):
                    temp[t] = 3
                    result[t] = 1
                else:
                    temp[t] = 0
                    result[t] = 0

            elif exit_level[t] < 0 and total_signal[t] != -3 and slowest_ma[t] < 0:
                if (
                    temp[counter - 1] == -3
                    and exit_level[t] == -1
                    and exit_level[counter - 1] == -1
                    and slowest_ma[t] == -1
                ):
                    temp[t] = -3
                    result[t] = -1
                else:
                    temp[t] = 0
                    result[t] = 0
            elif exit_level[t] > 0 or slowest_ma[t] > 0 and total_signal[t] != 3:
                temp[t] = 0
                result[t] = 0

            else:
                temp[t] = 0
                result[t] = 0

            # Mark exits after result round has finished
            if result[t] != prev_result:
                if result[t] >= 0 and prev_result == -1:
                    short_exits[t] = True
                if result[t] <= 0 and prev_result == 1:
                    exits[t] = True
        return result, entries, exits, short_entries, short_exits
