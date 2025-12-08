import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

# Setup structured logging
logger = logging.getLogger("RiskCore")

# --- Custom Exceptions ---
class RiskException(Exception):
    """Base exception for risk violations."""
    pass

class DailyLossLimitExceeded(RiskException):
    """Critical: Daily drawdown limit hit. Immediate HALT required."""
    pass

class MaxDrawdownExceeded(RiskException):
    """Critical: Total account drawdown hit. Immediate HALT required."""
    pass

class OrderRejectedRisk(RiskException):
    """Warning: Specific order rejected due to sizing or exposure limits."""
    pass


# --- Configuration Data Class ---
@dataclass(frozen=True)
class RiskConfig:
    """
    Immutable configuration for Prop Firm constraints.
    Using Decimal for financial precision.
    """
    max_daily_loss: Decimal      # e.g., 500.00
    max_total_loss: Decimal      # e.g., 1000.00
    max_position_size: Decimal   # e.g., 10000.00 (Notional value)
    max_leverage: Decimal        # e.g., 1.0 (No leverage for initial phases)
    daily_reset_hour: int = 17   # 5 PM EST (standard market close reset)


class RiskManager:
    """
    The Local Kill-Switch.
    Maintains internal state of P&L to act faster than Broker API callbacks.
    """

    def __init__(self, config: RiskConfig, initial_balance: float):
        self.config = config
        
        # Internal State (converted to Decimal for precision)
        self._initial_balance = Decimal(str(initial_balance))
        self._current_balance = Decimal(str(initial_balance))
        self._starting_day_balance = Decimal(str(initial_balance))
        
        self._current_daily_pnl = Decimal("0.00")
        self._current_total_pnl = Decimal("0.00")
        
        # The Circuit Breaker: If True, system is strictly locked.
        self._circuit_breaker_tripped: bool = False
        
        logger.info(f"RiskManager initialized. Daily Limit: -${config.max_daily_loss}")

    @property
    def is_tripped(self) -> bool:
        return self._circuit_breaker_tripped

    def update_pnl(self, current_equity: float, current_day_pnl: Optional[float] = None) -> None:
        """
        Updates internal state based on Broker telemetry.
        Must be called on every 'bar' or 'account_update' event.
        """
        if self._circuit_breaker_tripped:
            return  # System is dead, stop updating.

        equity_dec = Decimal(str(current_equity))
        
        # Calculate Total PnL
        self._current_total_pnl = equity_dec - self._initial_balance
        self._current_balance = equity_dec

        # Calculate Daily PnL
        # Note: Ideally the broker provides this (Alpaca does). 
        # If not, we calculate diff from self._starting_day_balance
        if current_day_pnl is not None:
            self._current_daily_pnl = Decimal(str(current_day_pnl))
        else:
            self._current_daily_pnl = equity_dec - self._starting_day_balance

        # Perform the Critical Check
        try:
            self.check_risk_status()
        except RiskException as e:
            logger.critical(f"RISK VIOLATION DETECTED DURING UPDATE: {e}")
            raise e

    def check_risk_status(self) -> None:
        """
        Validates current PnL against hard limits.
        Raises Critical Exceptions if limits are breached.
        """
        # 1. Check Daily Loss Limit
        # Note: We check if PnL is less than NEGATIVE limit (e.g. -500)
        if self._current_daily_pnl <= -(self.config.max_daily_loss):
            self._circuit_breaker_tripped = True
            msg = (f"Daily Loss Limit Hit! PnL: {self._current_daily_pnl} "
                   f"< Limit: -{self.config.max_daily_loss}")
            raise DailyLossLimitExceeded(msg)

        # 2. Check Max Total Drawdown
        if self._current_total_pnl <= -(self.config.max_total_loss):
            self._circuit_breaker_tripped = True
            msg = (f"Max Total Drawdown Hit! PnL: {self._current_total_pnl} "
                   f"< Limit: -{self.config.max_total_loss}")
            raise MaxDrawdownExceeded(msg)

    def can_execute_trade(self, trade_size_notional: float, current_position_notional: float = 0.0) -> bool:
        """
        Pre-trade validation gate.
        Args:
            trade_size_notional: The $ value of the proposed trade.
            current_position_notional: The $ value of existing positions (for adding to size).
        """
        if self._circuit_breaker_tripped:
            logger.warning("Trade attempted while Circuit Breaker is TRIPPED.")
            return False

        size_dec = Decimal(str(trade_size_notional))
        current_pos_dec = Decimal(str(current_position_notional))
        total_exposure = size_dec + current_pos_dec

        # 1. Check Logic: Account Health
        try:
            self.check_risk_status()
        except RiskException:
            return False

        # 2. Check Logic: Position Sizing
        if total_exposure > self.config.max_position_size:
            msg = (f"Trade rejected. Exposure {total_exposure} "
                   f"exceeds max size {self.config.max_position_size}")
            logger.warning(msg)
            raise OrderRejectedRisk(msg)

        # 3. Check Logic: Leverage (Simple check)
        current_leverage = (self._current_balance + size_dec) / self._current_balance
        # Note: This is a simplified leverage check; real systems check margin requirements.
        if current_leverage > self.config.max_leverage + Decimal("0.1"): # slight buffer
             msg = f"Trade rejected. Leverage {current_leverage} exceeds limit."
             logger.warning(msg)
             raise OrderRejectedRisk(msg)

        return True

    def reset_daily_stats(self, current_equity: float):
        """
        Called by the Scheduler at market open/close (e.g., 5 PM EST)
        to reset the daily PnL calculation anchor.
        """
        # Do not reset if the account is blown (Max Total Loss)
        if self._current_total_pnl <= -(self.config.max_total_loss):
            logger.error("Cannot reset daily stats: Account is blown.")
            return

        logger.info("Resetting Daily Risk Metrics...")
        self._starting_day_balance = Decimal(str(current_equity))
        self._current_daily_pnl = Decimal("0.00")
        
        # Only untrip if we haven't hit the TOTAL loss
        if self._current_daily_pnl > -(self.config.max_total_loss):
            self._circuit_breaker_tripped = False