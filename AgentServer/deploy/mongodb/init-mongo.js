// MongoDB 初始化脚本
// 创建数据库、用户和集合索引

// 切换到 stock_agent 数据库
db = db.getSiblingDB('stock_agent');

// ==================== 用户相关 ====================

db.users.createIndex({ "username": 1 }, { unique: true });
db.users.createIndex({ "email": 1 }, { unique: true });
db.users.createIndex({ "created_at": -1 });

// ==================== 股票基础数据 ====================

db.stock_basic.createIndex({ "ts_code": 1 }, { unique: true });
db.stock_basic.createIndex({ "industry": 1 });
db.stock_basic.createIndex({ "market": 1 });
db.stock_basic.createIndex({ "list_status": 1 });

// ==================== 日线数据 ====================

db.stock_daily.createIndex({ "ts_code": 1, "trade_date": -1 }, { unique: true });
db.stock_daily.createIndex({ "trade_date": -1 });

// ==================== 每日指标 ====================

db.daily_basic.createIndex({ "ts_code": 1, "trade_date": -1 }, { unique: true });
db.daily_basic.createIndex({ "trade_date": -1 });
db.daily_basic.createIndex({ "pe_ttm": 1 });
db.daily_basic.createIndex({ "pb": 1 });
db.daily_basic.createIndex({ "total_mv": -1 });

// ==================== 指数数据 ====================

db.index_basic.createIndex({ "ts_code": 1 }, { unique: true });
db.index_daily.createIndex({ "ts_code": 1, "trade_date": -1 }, { unique: true });

// ==================== 板块资金流 ====================

db.moneyflow_industry.createIndex({ "ts_code": 1, "trade_date": -1 }, { unique: true });
db.moneyflow_industry.createIndex({ "trade_date": -1 });

db.moneyflow_concept.createIndex({ "ts_code": 1, "trade_date": -1 }, { unique: true });
db.moneyflow_concept.createIndex({ "trade_date": -1 });

// ==================== 涨跌停 ====================

db.limit_list.createIndex({ "ts_code": 1, "trade_date": -1 }, { unique: true });
db.limit_list.createIndex({ "trade_date": -1 });
db.limit_list.createIndex({ "limit": 1 });

// ==================== 财务数据 ====================

db.fina_indicator.createIndex({ "ts_code": 1, "end_date": -1 });
db.fina_indicator.createIndex({ "end_date": -1 });

// ==================== 新闻数据 ====================

db.news.createIndex({ "datetime": -1 });
db.news.createIndex({ "src": 1, "datetime": -1 });

db.hot_news.createIndex({ "source_id": 1, "updated_at": -1 });
db.hot_news.createIndex({ "updated_at": -1 });

// ==================== 分析任务 ====================

db.analysis_tasks.createIndex({ "user_id": 1, "created_at": -1 });
db.analysis_tasks.createIndex({ "status": 1 });
db.analysis_tasks.createIndex({ "task_type": 1 });
db.analysis_tasks.createIndex({ "trace_id": 1 });

// ==================== 回测任务 ====================

db.backtest_tasks.createIndex({ "task_id": 1 }, { unique: true });
db.backtest_tasks.createIndex({ "params.user_id": 1, "created_at": -1 });
db.backtest_tasks.createIndex({ "status": 1 });

// ==================== 策略 ====================

db.strategies.createIndex({ "user_id": 1 });
db.strategies.createIndex({ "is_public": 1 });

// ==================== 同步记录 ====================

db.sync_records.createIndex({ "sync_type": 1, "sync_date": -1 });

print("MongoDB indexes created successfully!");
