"""
Stratos1 - Trade management modules.

Exports the manager classes for convenient imports::

    from managers import PositionManager, HedgeManager, ...
"""

from managers.hedge_manager import HedgeManager
from managers.position_manager import PositionManager
from managers.reentry_manager import ReentryManager
from managers.scaling_manager import ScalingManager

__all__ = [
    "PositionManager",
    "ScalingManager",
    "HedgeManager",
    "ReentryManager",
]
