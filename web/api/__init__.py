# -*- coding: utf-8 -*-
"""
API 模块
=======

提供 RESTful API 接口
"""
from flask import Blueprint

# 创建 API Blueprint
api_bp = Blueprint('api', __name__, url_prefix='/api')

# 导入所有路由（必须在 Blueprint 创建之后）
from . import intervals
from . import addresses
from . import netvalue
from . import stats
from . import cache
from . import export
from . import calculate

__all__ = ['api_bp']

