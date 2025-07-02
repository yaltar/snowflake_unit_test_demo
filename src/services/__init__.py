"""
Services package for business logic.

This package contains service classes that encapsulate business logic
using SQL queries through the Querier interface. Services provide a
clean Python API for business operations while abstracting database
interactions.
"""

from .analytics_service import RevenueAnalytics, OrderAnalytics, ProductAnalytics

__all__ = ['RevenueAnalytics', 'OrderAnalytics', 'ProductAnalytics']