"""
Stratos1 - Trade management modules.

Exports all manager classes for convenient imports::

    from managers import PositionManager, BreakevenManager, ...
"""

from managers.breakeven_manager import BreakevenManager
from managers.hedge_manager import HedgeManager
from managers.position_manager import PositionManager
from managers.reentry_manager import ReentryManager
from managers.scaling_manager import ScalingManager
from managers.trailing_manager import TrailingManager

__all__ = [
    "PositionManager",
    "BreakevenManager",
    "ScalingManager",
    "TrailingManager",
    "HedgeManager",
    "ReentryManager",
]
