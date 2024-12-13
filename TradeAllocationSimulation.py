import dataiku
import pandas as pd
from datetime import timedelta

class TradeAllocationSimulation:
    """
    A class to simulate trade allocation strategies with optional buffering.
    """
    def __init__(self, file_path, strategy='FirstArrive', use_buffer=False, buffer_threshold_minutes=60):
        """
        Initialize the simulation.

        :param file_path: Path to the trade data file (CSV).
        :param strategy: 'FirstArrive' or 'MaxMW'.
        :param use_buffer: Whether to use a buffer in the simulation.
        :param buffer_threshold_minutes: Urgency threshold for buffering (default: 60 minutes).
        """
        self.file_path = file_path
        self.strategy = strategy
        self.use_buffer = use_buffer
        self.buffer_threshold = timedelta(minutes=buffer_threshold_minutes)
        self.trade_limits = {
            'Hourly': 48,
            'HalfHourly': 96,
            'QuarterHourly': 192
        }
        self.max_messages_per_minute = 60
        self.data = self._load_data()

    def _load_data(self):
        """
        Load and preprocess trade data.
        """
        data = dataiku.Dataset(self.file_path).get_dataframe()
        data['TransactionTime_parsed'] = pd.to_datetime(data['TransactionTime_parsed'])
        data['ProductFromUTC'] = pd.to_datetime(data['ProductFromUTC'])
        data['ProductCategory'] = data['ProductTimeDiffMinutes'].map(
            {15: 'QuarterHourly', 30: 'HalfHourly', 60: 'Hourly'}
        )
        return data

    def simulate_allocation(self):
        """
        Simulate trade allocation based on the chosen strategy and buffering option.

        :return: Unallocated trades as a DataFrame.
        """
        allocated_trades = []
        unallocated_trades = []

        # Group trades by minute
        self.data['Minute'] = self.data['TransactionTime_parsed'].dt.floor('T')
        grouped = self.data.groupby('Minute')

        for _, group in grouped:
            buffer = []
            message_count = 0

            # If using a buffer, filter trades based on urgency threshold
            if self.use_buffer:
                group['Urgency'] = (group['ProductFromUTC'] - group['TransactionTime_parsed']).abs()
                buffer.extend(group[group['Urgency'] <= self.buffer_threshold].to_dict('records'))
            else:
                buffer.extend(group.to_dict('records'))

            # Apply strategy (FirstArrive or MaxMW)
            if self.strategy == 'MaxMW':
                buffer = sorted(buffer, key=lambda x: -x['QuantityMWh'])

            # Flush buffer based on trade limits and message constraints
            for product_type, limit in self.trade_limits.items():
                product_group = [trade for trade in buffer if trade['ProductCategory'] == product_type]
                remaining_limit = max(0, self.max_messages_per_minute - message_count)

                # Allocate trades within the remaining limit
                allocated = product_group[:min(limit, remaining_limit)]
                unallocated = product_group[len(allocated):]

                # Update allocation lists
                allocated_trades.extend(allocated)
                unallocated_trades.extend(unallocated)

                # Update message count and remove allocated trades from buffer
                message_count += len(allocated)
                buffer = [trade for trade in buffer if trade not in allocated]

        # Convert unallocated trades to DataFrame
        return pd.DataFrame(unallocated_trades)

    def get_summary(self):
        """
        Get a summary of unallocated trades.

        :return: Summary DataFrame with total unallocated MW and trade counts.
        """
        unallocated_df = self.simulate_allocation()
        unallocated_summary = unallocated_df.groupby('ProductCategory')['QuantityMWh'].sum()
        unallocated_counts = unallocated_df['ProductCategory'].value_counts()
        return pd.DataFrame({
            'Unallocated MWh': unallocated_summary,
            'Unallocated Trades': unallocated_counts
        })


# Save this code to a file for execution on your local machine. Use it like this:
# sim = TradeAllocationSimulation(file_path='path/to/your/data.csv', strategy='MaxMW', use_buffer=True, buffer_threshold_minutes=120)
# summary = sim.get_summary()
# print(summary)
