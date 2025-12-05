# -*- coding: utf-8 -*-
"""
净值数据库管理器 - TimescaleDB 版本
====================================

功能：
1. 使用 TimescaleDB 存储净值数据（自动分区超表）
2. 支持增量更新（向后追加数据）
3. 自动创建超表和压缩策略
4. 高性能查询和存储
"""

import sys
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
from typing import Optional, List, Dict
import os
import warnings

# 抑制 Pandas SQLAlchemy 警告
warnings.filterwarnings('ignore', 
    message='pandas only supports SQLAlchemy',
    category=UserWarning)

# 注意：编码设置应该在程序入口点（如 app.py 或 run_*.py）进行
# 不要在被导入的模块中重复设置，否则会导致 "I/O operation on closed file" 错误


class NetValueTimescaleManager:
    """净值数据库管理器 - TimescaleDB 版本"""
    
    # 时间区间与表名的映射
    INTERVAL_TABLE_MAP = {
        '1m': 'net_value_1m',
        '3m': 'net_value_3m',
        '5m': 'net_value_5m',
        '15m': 'net_value_15m',
        '30m': 'net_value_30m',
        '1h': 'net_value_1h',
        '2h': 'net_value_2h',
        '4h': 'net_value_4h',
        '8h': 'net_value_8h',
        '12h': 'net_value_12h',
        '1d': 'net_value_1d',
    }
    
    # 更新记录表名
    UPDATE_RECORD_TABLE = 'net_value_update_records'
    
    def __init__(self, host: str = 'localhost', port: int = 5432, 
                 database: str = 'trading', user: str = 'postgres', 
                 password: str = 'password'):
        """
        初始化数据库管理器
        
        参数:
            host: 数据库主机地址
            port: 数据库端口
            database: 数据库名称
            user: 用户名
            password: 密码
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        
        # 测试连接
        self._test_connection()
    
    def _test_connection(self):
        """测试数据库连接"""
        try:
            conn = self._get_connection()
            # 测试连接的同时，确保更新记录表存在
            self._create_update_record_table_if_not_exists(conn)
            conn.close()
            print(f"✅ 成功连接到 TimescaleDB: {self.host}:{self.port}/{self.database}", flush=True)
        except Exception as e:
            print(f"❌ 无法连接到 TimescaleDB: {e}", flush=True)
            print(f"   请确保 TimescaleDB 正在运行并且连接信息正确", flush=True)
            raise
    
    def _get_connection(self):
        """获取数据库连接"""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
    
    def _create_update_record_table_if_not_exists(self, conn):
        """
        创建更新记录表（如果不存在）
        
        表结构：
        - address: 账户地址（主键）
        - first_trade_timestamp: 第一笔交易时间戳（毫秒）
        - time_1m: 1分钟数据最后更新时间
        - time_3m: 3分钟数据最后更新时间
        - ... (所有时间周期)
        - time_1d: 1天数据最后更新时间
        
        参数:
            conn: 数据库连接
        """
        cursor = conn.cursor()
        
        try:
            # 检查表是否存在
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (self.UPDATE_RECORD_TABLE,))
            
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                print(f"   创建更新记录表: {self.UPDATE_RECORD_TABLE}", flush=True)
                
                # 动态构建列定义
                columns = ["address TEXT PRIMARY KEY"]
                columns.append("first_trade_timestamp BIGINT")  # 第一笔交易时间戳（毫秒）
                for interval in self.INTERVAL_TABLE_MAP.keys():
                    # 将 1m, 3m 等转换为 time_1m, time_3m
                    column_name = f"time_{interval}"
                    columns.append(f"{column_name} TIMESTAMPTZ")
                
                columns_sql = ",\n                        ".join(columns)
                
                # 创建表
                create_table_sql = f"""
                    CREATE TABLE {self.UPDATE_RECORD_TABLE} (
                        {columns_sql}
                    )
                """
                cursor.execute(create_table_sql)
                
                # 创建索引
                cursor.execute(f"""
                    CREATE INDEX {self.UPDATE_RECORD_TABLE}_address_idx 
                    ON {self.UPDATE_RECORD_TABLE} (address)
                """)
                
                conn.commit()
                print(f"✅ 更新记录表 {self.UPDATE_RECORD_TABLE} 创建成功", flush=True)
            else:
                # 表已存在，检查是否有 first_trade_timestamp 列
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_name = %s AND column_name = 'first_trade_timestamp'
                    )
                """, (self.UPDATE_RECORD_TABLE,))
                
                column_exists = cursor.fetchone()[0]
                
                if not column_exists:
                    print(f"   添加 first_trade_timestamp 列到 {self.UPDATE_RECORD_TABLE}", flush=True)
                    cursor.execute(f"""
                        ALTER TABLE {self.UPDATE_RECORD_TABLE}
                        ADD COLUMN first_trade_timestamp BIGINT
                    """)
                    conn.commit()
                    print(f"✅ 列 first_trade_timestamp 添加成功", flush=True)
            
        except Exception as e:
            conn.rollback()
            print(f"⚠️  创建更新记录表失败: {e}", flush=True)
            # 不抛出异常，让程序继续运行
    
    def _get_table_name(self, interval: str) -> str:
        """
        获取表名
        
        参数:
            interval: 时间区间（如 '1h', '1d'）
        
        返回:
            表名
        """
        if interval not in self.INTERVAL_TABLE_MAP:
            raise ValueError(f"不支持的时间区间: {interval}，支持的区间: {list(self.INTERVAL_TABLE_MAP.keys())}")
        
        return self.INTERVAL_TABLE_MAP[interval]
    
    def _create_hypertable_if_not_exists(self, conn, table_name: str, interval: str):
        """
        创建超表（如果不存在）
        
        参数:
            conn: 数据库连接
            table_name: 表名
            interval: 时间区间（用于设置分块大小）
        """
        cursor = conn.cursor()
        
        try:
            # 检查表是否存在
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table_name,))
            
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                print(f"   创建表: {table_name}", flush=True)
                
                # 创建普通表
                cursor.execute(sql.SQL("""
                    CREATE TABLE {} (
                        address TEXT NOT NULL,
                        time TIMESTAMPTZ NOT NULL,
                        spot_account_value DOUBLE PRECISION,
                        realized_pnl DOUBLE PRECISION,
                        virtual_pnl DOUBLE PRECISION,
                        perp_account_value DOUBLE PRECISION,
                        total_assets DOUBLE PRECISION,
                        total_shares DOUBLE PRECISION,
                        net_value DOUBLE PRECISION,
                        cumulative_pnl DOUBLE PRECISION
                    )
                """).format(sql.Identifier(table_name)))
                
                # 确定分块大小
                chunk_time_interval = self._get_chunk_interval(interval)
                
                # 转换为超表
                print(f"   转换为超表（分块间隔: {chunk_time_interval}）...", flush=True)
                cursor.execute(sql.SQL("""
                    SELECT create_hypertable(
                        %s, 
                        'time',
                        chunk_time_interval => INTERVAL %s
                    )
                """), (table_name, chunk_time_interval))
                
                # 创建唯一索引（用于防止重复数据）
                cursor.execute(sql.SQL("""
                    CREATE UNIQUE INDEX {} 
                    ON {} (address, time DESC)
                """).format(
                    sql.Identifier(f"{table_name}_address_time_idx"),
                    sql.Identifier(table_name)
                ))
                
                print(f"   创建索引...", flush=True)
                
                # 创建地址索引
                cursor.execute(sql.SQL("""
                    CREATE INDEX {} 
                    ON {} (address)
                """).format(
                    sql.Identifier(f"{table_name}_address_idx"),
                    sql.Identifier(table_name)
                ))
                
                # 添加压缩策略（旧数据自动压缩）
                print(f"   添加压缩策略（7天后自动压缩）...", flush=True)
                cursor.execute(sql.SQL("""
                    ALTER TABLE {} SET (
                        timescaledb.compress,
                        timescaledb.compress_segmentby = 'address'
                    )
                """).format(sql.Identifier(table_name)))
                
                cursor.execute(sql.SQL("""
                    SELECT add_compression_policy(%s, INTERVAL '7 days')
                """), (table_name,))
                
                # 添加数据保留策略（可选，默认不删除）
                # cursor.execute(sql.SQL("""
                #     SELECT add_retention_policy(%s, INTERVAL '2 years')
                # """), (table_name,))
                
                conn.commit()
                print(f"✅ 超表 {table_name} 创建成功", flush=True)
            
        except Exception as e:
            conn.rollback()
            print(f"⚠️  创建超表失败: {e}", flush=True)
            raise
    
    def _get_chunk_interval(self, interval: str) -> str:
        """
        根据时间区间确定分块大小
        
        参数:
            interval: 时间区间
        
        返回:
            PostgreSQL interval 字符串
        """
        # 分块策略：存储约 1000-2000 条记录的时间跨度
        chunk_map = {
            '1m': '1 day',      # 1440 条/天
            '3m': '3 days',     # 1440 条/3天
            '5m': '5 days',     # 1440 条/5天
            '15m': '7 days',    # 672 条/周
            '30m': '14 days',   # 672 条/两周
            '1h': '1 month',    # 720 条/月
            '2h': '2 months',   # 720 条/两月
            '4h': '3 months',   # 540 条/季度
            '8h': '6 months',   # 540 条/半年
            '12h': '1 year',    # 730 条/年
            '1d': '1 year',     # 365 条/年
        }
        return chunk_map.get(interval, '7 days')
    
    def get_latest_timestamp(self, address: str, interval: str) -> Optional[int]:
        """
        获取指定地址的最新时间戳
        
        参数:
            address: 账户地址
            interval: 时间区间
        
        返回:
            最新时间戳（毫秒，如果没有数据则返回 None）
        """
        table_name = self._get_table_name(interval)
        
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            # 查询最大时间戳
            cursor.execute(sql.SQL("""
                SELECT EXTRACT(EPOCH FROM MAX(time)) * 1000
                FROM {}
                WHERE address = %s
            """).format(sql.Identifier(table_name)), (address,))
            
            result = cursor.fetchone()
            return int(result[0]) if result[0] is not None else None
            
        finally:
            conn.close()
    
    def save_net_value_data(
        self, 
        address: str, 
        interval: str, 
        df: pd.DataFrame,
        incremental: bool = True
    ) -> Dict[str, int]:
        """
        保存净值数据到数据库
        
        参数:
            address: 账户地址
            interval: 时间区间
            df: 包含净值数据的 DataFrame（必须包含指定的列）
            incremental: 是否增量更新（True: 只追加新数据，False: 覆盖所有数据）
        
        返回:
            统计信息字典 {'inserted': 插入数量, 'skipped': 跳过数量, 'total': 总数量}
        """
        # 验证必需的列
        required_columns = [
            'timestamp', 'spot_account_value', 'realized_pnl', 'virtual_pnl',
            'perp_account_value', 'total_assets', 'total_shares', 'net_value',
            'cumulative_pnl'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"DataFrame 缺少必需的列: {missing_columns}")
        
        # 获取表名
        table_name = self._get_table_name(interval)
        
        # 连接数据库
        conn = self._get_connection()
        
        try:
            # 创建超表
            self._create_hypertable_if_not_exists(conn, table_name, interval)
            
            cursor = conn.cursor()
            
            # 如果是增量更新，获取最新时间戳（直接在当前连接中查询，避免创建新连接）
            latest_timestamp = None
            if incremental:
                cursor.execute(sql.SQL("""
                    SELECT EXTRACT(EPOCH FROM MAX(time)) * 1000
                    FROM {}
                    WHERE address = %s
                """).format(sql.Identifier(table_name)), (address,))
                
                result = cursor.fetchone()
                latest_timestamp = int(result[0]) if result[0] is not None else None
                
                if latest_timestamp is not None:
                    print(f"   数据库中已有数据，最新时间戳: {latest_timestamp}", flush=True)
            
            # 准备要插入的数据
            df_to_insert = df.copy()
            
            # 确保 timestamp 是整数类型
            df_to_insert['timestamp'] = df_to_insert['timestamp'].astype('int64')
            
            # 如果是增量更新且有历史数据，只保留新数据
            skipped_count = 0
            if incremental and latest_timestamp is not None:
                original_count = len(df_to_insert)
                df_to_insert = df_to_insert[df_to_insert['timestamp'] > latest_timestamp]
                skipped_count = original_count - len(df_to_insert)
                
                if skipped_count > 0:
                    print(f"   跳过已存在的 {skipped_count} 条数据", flush=True)
            
            # 如果没有新数据，更新记录表后返回
            if len(df_to_insert) == 0:
                print(f"   ℹ️  没有新数据需要插入", flush=True)
                
                # 即使没有新数据，也更新记录表（记录检查时间）
                import time
                current_timestamp = int(time.time() * 1000)
                try:
                    self.update_record_time(address, interval, current_timestamp)
                    print(f"   ✅ 更新记录时间: {interval} -> {current_timestamp} (当前时间，无新数据)", flush=True)
                except Exception as e:
                    print(f"   ⚠️  更新记录时间失败: {e}", flush=True)
                
                return {'inserted': 0, 'skipped': skipped_count, 'total': len(df)}
            
            # 如果不是增量更新，先删除该地址的所有数据
            if not incremental:
                cursor.execute(sql.SQL("DELETE FROM {} WHERE address = %s").format(
                    sql.Identifier(table_name)
                ), (address,))
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    print(f"   已删除 {deleted_count} 条旧数据", flush=True)
            
            # 准备批量插入数据
            print(f"   开始插入 {len(df_to_insert)} 条数据...", flush=True)
            
            # 使用向量化操作准备数据（比 iterrows 快10-100倍）
            # 将 NaN 替换为 None
            df_prepared = df_to_insert[['timestamp', 'spot_account_value', 'realized_pnl', 
                                        'virtual_pnl', 'perp_account_value', 'total_assets',
                                        'total_shares', 'net_value', 'cumulative_pnl']].copy()
            df_prepared = df_prepared.where(pd.notna(df_prepared), None)
            
            # 转换为列表（向量化操作，非常快）
            values_list = df_prepared.values.tolist()
            
            # 为每行添加 address（使用列表推导式，比循环快）
            data_list = [(address, int(row[0]), *row[1:]) for row in values_list]
            
            # 使用 execute_values 批量插入（高性能）
            insert_query_str = sql.SQL("""
                INSERT INTO {} (address, time, spot_account_value, realized_pnl, 
                               virtual_pnl, perp_account_value, total_assets, 
                               total_shares, net_value, cumulative_pnl)
                VALUES %s
                ON CONFLICT (address, time) DO NOTHING
            """).format(sql.Identifier(table_name)).as_string(conn)
            
            # 批量插入（每批1000条，每5批commit一次以提高性能）
            batch_size = 1000
            commit_interval = 5  # 每5批commit一次
            inserted_count = 0
            batches_since_commit = 0
            
            for i in range(0, len(data_list), batch_size):
                batch_data = data_list[i:i+batch_size]
                
                # 使用 execute_values 批量插入
                execute_values(
                    cursor,
                    insert_query_str,
                    batch_data,
                    template="(%s, to_timestamp(%s::double precision / 1000), %s, %s, %s, %s, %s, %s, %s, %s)"
                )
                
                batches_since_commit += 1
                inserted_count += len(batch_data)
                
                # 每5批或最后一批时提交事务
                if batches_since_commit >= commit_interval or (i + batch_size) >= len(data_list):
                    conn.commit()
                    batches_since_commit = 0
                
                # 显示进度
                progress_pct = (min(i + batch_size, len(data_list)) / len(data_list)) * 100
                print(f"   已插入 {min(i + batch_size, len(data_list))}/{len(data_list)} 条数据 ({progress_pct:.1f}%)...", flush=True)
            
            # 插入数据后，更新记录表
            # 使用当前时间作为更新时间（记录什么时候执行的更新操作）
            import time
            current_timestamp = int(time.time() * 1000)  # 当前时间（毫秒）
            latest_data_timestamp = int(df_to_insert['timestamp'].max())  # 数据的最新时间
            
            try:
                self.update_record_time(address, interval, current_timestamp)
                print(f"   ✅ 更新记录时间: {interval} -> {current_timestamp} (当前时间)", flush=True)
                print(f"   📊 数据最新时间: {latest_data_timestamp}", flush=True)
            except Exception as e:
                print(f"   ⚠️  更新记录时间失败: {e}", flush=True)
            
            return {
                'inserted': inserted_count,
                'skipped': skipped_count,
                'total': len(df)
            }
            
        except Exception as e:
            conn.rollback()
            raise Exception(f"保存数据到数据库失败: {e}")
        
        finally:
            conn.close()
    
    def query_net_value_data(
        self,
        address: str,
        interval: str,
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None
    ) -> pd.DataFrame:
        """
        查询净值数据
        
        参数:
            address: 账户地址
            interval: 时间区间
            start_timestamp: 起始时间戳（毫秒，可选）
            end_timestamp: 结束时间戳（毫秒，可选）
        
        返回:
            包含净值数据的 DataFrame
        """
        table_name = self._get_table_name(interval)
        
        conn = self._get_connection()
        
        try:
            # 构建查询语句（使用字符串拼接构建动态WHERE条件）
            where_conditions = ["address = %s"]
            params = [address]
            
            if start_timestamp is not None:
                where_conditions.append("time >= to_timestamp(%s::double precision / 1000)")
                params.append(start_timestamp)
            
            if end_timestamp is not None:
                where_conditions.append("time <= to_timestamp(%s::double precision / 1000)")
                params.append(end_timestamp)
            
            # 只筛选有效数据（total_shares > 0）
            where_conditions.append("total_shares > 0")
            
            where_clause = " AND ".join(where_conditions)
            
            # 构建完整SQL（使用sql.SQL确保安全）
            # 使用窗口函数在SQL层面过滤：找到第一个非零 cumulative_pnl，从该点开始返回数据
            query = sql.SQL("""
                WITH ranked_data AS (
                    SELECT 
                        address,
                        EXTRACT(EPOCH FROM time) * 1000 as timestamp,
                        spot_account_value,
                        realized_pnl,
                        virtual_pnl,
                        perp_account_value,
                        total_assets,
                        total_shares,
                        net_value,
                        cumulative_pnl,
                        ROW_NUMBER() OVER (ORDER BY time ASC) as rn
                    FROM {}
                    WHERE {}
                ),
                first_nonzero AS (
                    SELECT MIN(rn) as min_rn
                    FROM ranked_data
                    WHERE ABS(cumulative_pnl) > 0.000001
                )
                SELECT 
                    address,
                    timestamp,
                    spot_account_value,
                    realized_pnl,
                    virtual_pnl,
                    perp_account_value,
                    total_assets,
                    total_shares,
                    net_value,
                    cumulative_pnl
                FROM ranked_data
                WHERE rn >= COALESCE((SELECT min_rn FROM first_nonzero), 1)
                ORDER BY timestamp ASC
            """).format(
                sql.Identifier(table_name),
                sql.SQL(where_clause)
            )
            query_str = query.as_string(conn)
            
            # 查询数据（pd.read_sql_query会自动处理参数化查询）
            df = pd.read_sql_query(query_str, conn, params=params)
            
            return df
            
        finally:
            conn.close()
    
    def get_table_stats(self, interval: str) -> Dict:
        """
        获取表的统计信息
        
        参数:
            interval: 时间区间
        
        返回:
            统计信息字典
        """
        table_name = self._get_table_name(interval)
        
        conn = self._get_connection()
        
        try:
            cursor = conn.cursor()
            
            # 检查表是否存在
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table_name,))
            
            if not cursor.fetchone()[0]:
                return {
                    'exists': False,
                    'total_records': 0,
                    'address_count': 0,
                    'earliest_timestamp': None,
                    'latest_timestamp': None,
                    'chunks': 0,
                    'compressed_chunks': 0,
                    'total_size': '0 B',
                    'compressed_size': '0 B'
                }
            
            # 获取总记录数
            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(table_name)
            ))
            total_records = cursor.fetchone()[0]
            
            # 获取地址数量
            cursor.execute(sql.SQL("SELECT COUNT(DISTINCT address) FROM {}").format(
                sql.Identifier(table_name)
            ))
            address_count = cursor.fetchone()[0]
            
            # 获取时间范围
            cursor.execute(sql.SQL("""
                SELECT 
                    EXTRACT(EPOCH FROM MIN(time)) * 1000,
                    EXTRACT(EPOCH FROM MAX(time)) * 1000
                FROM {}
            """).format(sql.Identifier(table_name)))
            result = cursor.fetchone()
            earliest_timestamp = int(result[0]) if result[0] is not None else None
            latest_timestamp = int(result[1]) if result[1] is not None else None
            
            # 获取分块信息
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_chunks,
                    COUNT(*) FILTER (WHERE is_compressed) as compressed_chunks
                FROM timescaledb_information.chunks
                WHERE hypertable_name = %s
            """, (table_name,))
            chunks_result = cursor.fetchone()
            total_chunks = chunks_result[0] if chunks_result[0] else 0
            compressed_chunks = chunks_result[1] if chunks_result[1] else 0
            
            # 获取表大小
            cursor.execute(sql.SQL("""
                SELECT 
                    pg_size_pretty(pg_total_relation_size(%s)) as total_size,
                    pg_size_pretty(
                        pg_total_relation_size(%s) - 
                        COALESCE(
                            (SELECT SUM(pg_total_relation_size(format('%%I.%%I', chunk_schema, chunk_name)))
                             FROM timescaledb_information.chunks
                             WHERE hypertable_name = %s AND NOT is_compressed),
                            0
                        )
                    ) as compressed_size
            """), (table_name, table_name, table_name))
            size_result = cursor.fetchone()
            
            return {
                'exists': True,
                'total_records': total_records,
                'address_count': address_count,
                'earliest_timestamp': earliest_timestamp,
                'latest_timestamp': latest_timestamp,
                'chunks': total_chunks,
                'compressed_chunks': compressed_chunks,
                'total_size': size_result[0] if size_result else '0 B',
                'compressed_size': size_result[1] if size_result else '0 B'
            }
            
        finally:
            conn.close()
    
    def list_addresses(self, interval: str) -> List[str]:
        """
        列出指定时间区间表中的所有地址
        
        参数:
            interval: 时间区间
        
        返回:
            地址列表
        """
        table_name = self._get_table_name(interval)
        
        conn = self._get_connection()
        
        try:
            cursor = conn.cursor()
            
            # 检查表是否存在
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table_name,))
            
            if not cursor.fetchone()[0]:
                return []
            
            # 查询所有地址
            cursor.execute(sql.SQL("SELECT DISTINCT address FROM {}").format(
                sql.Identifier(table_name)
            ))
            addresses = [row[0] for row in cursor.fetchall()]
            
            return addresses
            
        finally:
            conn.close()
    
    def list_all_addresses(self) -> Dict[str, List[str]]:
        """
        一次性列出所有时间区间表中的地址（批量查询，性能优化）
        
        优化：直接从更新记录表查询，而不是遍历11个表
        
        返回:
            字典，键为时间区间，值为地址列表
            例如: {'1h': ['addr1', 'addr2'], '1d': ['addr3']}
        """
        conn = self._get_connection()
        result = {}
        
        try:
            cursor = conn.cursor()
            
            # 检查更新记录表是否存在
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (self.UPDATE_RECORD_TABLE,))
            
            if cursor.fetchone()[0]:
                # 优化：直接从更新记录表查询（一次查询获取所有信息）
                # 构建查询列
                columns = ['address']
                for interval in self.INTERVAL_TABLE_MAP.keys():
                    columns.append(f"time_{interval}")
                
                columns_sql = ", ".join(columns)
                
                query_sql = f"""
                    SELECT {columns_sql}
                    FROM {self.UPDATE_RECORD_TABLE}
                """
                
                cursor.execute(query_sql)
                rows = cursor.fetchall()
                
                # 初始化结果字典
                for interval in self.INTERVAL_TABLE_MAP.keys():
                    result[interval] = []
                
                # 遍历每个地址，根据时间列是否为NULL判断该地址在哪些周期有数据
                for row in rows:
                    address = row[0]
                    for idx, interval in enumerate(self.INTERVAL_TABLE_MAP.keys(), start=1):
                        # row[idx] 是对应的 time_xx 列
                        if row[idx] is not None:  # 如果该周期有更新记录
                            result[interval].append(address)
                
                return result
            else:
                # 降级：如果更新记录表不存在，使用原来的方法
                print("⚠️  更新记录表不存在，使用传统方法查询地址", flush=True)
                
                for interval, table_name in self.INTERVAL_TABLE_MAP.items():
                    # 检查表是否存在
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = %s
                        )
                    """, (table_name,))
                    
                    if cursor.fetchone()[0]:
                        # 查询所有地址
                        cursor.execute(sql.SQL("SELECT DISTINCT address FROM {}").format(
                            sql.Identifier(table_name)
                        ))
                        addresses = [row[0] for row in cursor.fetchall()]
                        result[interval] = addresses
                    else:
                        result[interval] = []
                
                return result
            
        finally:
            conn.close()
    
    def update_record_time(self, address: str, interval: str, update_time: Optional[int] = None):
        """
        更新指定地址和时间周期的更新记录时间
        
        参数:
            address: 账户地址
            interval: 时间区间
            update_time: 更新时间（毫秒时间戳，如果为None则使用当前时间）
        """
        if interval not in self.INTERVAL_TABLE_MAP:
            raise ValueError(f"不支持的时间区间: {interval}")
        
        # 如果没有指定更新时间，使用当前时间
        if update_time is None:
            import time
            update_time = int(time.time() * 1000)
        
        conn = self._get_connection()
        
        try:
            cursor = conn.cursor()
            
            # 列名：time_1m, time_3m 等
            column_name = f"time_{interval}"
            
            # 调试信息：显示要更新的内容
            from datetime import datetime
            update_time_str = datetime.fromtimestamp(update_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
            print(f"   🔄 准备更新记录: {column_name} = {update_time_str} (地址: {address[:10]}...)", flush=True)
            
            # 使用 UPSERT（INSERT ... ON CONFLICT）语法
            # 如果地址不存在，插入新记录；如果存在，更新对应的时间列
            upsert_sql = f"""
                INSERT INTO {self.UPDATE_RECORD_TABLE} (address, {column_name})
                VALUES (%s, to_timestamp(%s::double precision / 1000))
                ON CONFLICT (address) 
                DO UPDATE SET {column_name} = EXCLUDED.{column_name}
            """
            
            cursor.execute(upsert_sql, (address, update_time))
            rows_affected = cursor.rowcount
            conn.commit()
            
            # 验证更新是否成功
            cursor.execute(f"""
                SELECT EXTRACT(EPOCH FROM {column_name}) * 1000 
                FROM {self.UPDATE_RECORD_TABLE}
                WHERE address = %s
            """, (address,))
            result = cursor.fetchone()
            
            if result and result[0]:
                actual_timestamp = int(result[0])
                actual_time_str = datetime.fromtimestamp(actual_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                
                if abs(actual_timestamp - update_time) < 1000:  # 允许1秒误差
                    print(f"   ✅ 更新成功验证: {column_name} = {actual_time_str}", flush=True)
                else:
                    print(f"   ⚠️  更新后验证失败: 期望 {update_time_str}，实际 {actual_time_str}", flush=True)
            else:
                print(f"   ⚠️  更新后查询为空", flush=True)
            
        except Exception as e:
            conn.rollback()
            print(f"   ❌ 更新记录时间失败: {e}", flush=True)
            import traceback
            traceback.print_exc()
            # 不抛出异常，避免影响主流程
        
        finally:
            conn.close()
    
    def get_update_record(self, address: str) -> Optional[Dict[str, Optional[int]]]:
        """
        获取指定地址的所有更新记录
        
        参数:
            address: 账户地址
        
        返回:
            字典，键为时间区间，值为最后更新时间（毫秒时间戳），如果未更新过则为None
            例如: {'1m': 1704067200000, '3m': None, '5m': 1704070800000, ...}
            如果地址不存在，返回None
        """
        conn = self._get_connection()
        
        try:
            cursor = conn.cursor()
            
            # 构建查询列
            columns = ["address"]
            for interval in self.INTERVAL_TABLE_MAP.keys():
                column_name = f"time_{interval}"
                # 转换为毫秒时间戳
                columns.append(f"EXTRACT(EPOCH FROM {column_name}) * 1000 as {column_name}")
            
            columns_sql = ", ".join(columns)
            
            query_sql = f"""
                SELECT {columns_sql}
                FROM {self.UPDATE_RECORD_TABLE}
                WHERE address = %s
            """
            
            cursor.execute(query_sql, (address,))
            result = cursor.fetchone()
            
            if result is None:
                return None
            
            # 构建返回字典
            record = {}
            for idx, interval in enumerate(self.INTERVAL_TABLE_MAP.keys(), start=1):
                # result[0] 是 address，从 result[1] 开始是时间列
                timestamp_value = result[idx]
                record[interval] = int(timestamp_value) if timestamp_value is not None else None
            
            return record
            
        finally:
            conn.close()
    
    def get_all_update_records(self) -> Dict[str, Dict[str, Optional[int]]]:
        """
        获取所有地址的更新记录
        
        返回:
            嵌套字典，外层键为地址，内层键为时间区间，值为最后更新时间（毫秒时间戳）
            例如: {
                '0x123...': {'1m': 1704067200000, '3m': None, ...},
                '0x456...': {'1m': 1704070800000, '3m': 1704074400000, ...}
            }
        """
        conn = self._get_connection()
        
        try:
            cursor = conn.cursor()
            
            # 构建查询列
            columns = ["address"]
            for interval in self.INTERVAL_TABLE_MAP.keys():
                column_name = f"time_{interval}"
                columns.append(f"EXTRACT(EPOCH FROM {column_name}) * 1000 as {column_name}")
            
            columns_sql = ", ".join(columns)
            
            query_sql = f"""
                SELECT {columns_sql}
                FROM {self.UPDATE_RECORD_TABLE}
                ORDER BY address
            """
            
            cursor.execute(query_sql)
            results = cursor.fetchall()
            
            # 构建返回字典
            all_records = {}
            for row in results:
                address = row[0]
                record = {}
                for idx, interval in enumerate(self.INTERVAL_TABLE_MAP.keys(), start=1):
                    timestamp_value = row[idx]
                    record[interval] = int(timestamp_value) if timestamp_value is not None else None
                all_records[address] = record
            
            return all_records
            
        finally:
            conn.close()
    
    def update_first_trade_timestamp(self, address: str, first_trade_timestamp: int):
        """
        更新指定地址的第一笔交易时间戳
        
        参数:
            address: 账户地址
            first_trade_timestamp: 第一笔交易时间戳（毫秒）
        """
        conn = self._get_connection()
        
        try:
            cursor = conn.cursor()
            
            # 使用 UPSERT
            upsert_sql = f"""
                INSERT INTO {self.UPDATE_RECORD_TABLE} (address, first_trade_timestamp)
                VALUES (%s, %s)
                ON CONFLICT (address) 
                DO UPDATE SET first_trade_timestamp = EXCLUDED.first_trade_timestamp
                WHERE {self.UPDATE_RECORD_TABLE}.first_trade_timestamp IS NULL
                   OR {self.UPDATE_RECORD_TABLE}.first_trade_timestamp > EXCLUDED.first_trade_timestamp
            """
            
            cursor.execute(upsert_sql, (address, first_trade_timestamp))
            conn.commit()
            
            from datetime import datetime
            time_str = datetime.fromtimestamp(first_trade_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
            print(f"   ✅ 更新第一笔交易时间: {time_str}", flush=True)
            
        except Exception as e:
            conn.rollback()
            print(f"   ⚠️  更新第一笔交易时间失败: {e}", flush=True)
        
        finally:
            conn.close()
    
    def get_first_trade_timestamp(self, address: str) -> Optional[int]:
        """
        获取指定地址的第一笔交易时间戳
        
        参数:
            address: 账户地址
        
        返回:
            第一笔交易时间戳（毫秒），如果不存在则返回 None
        """
        conn = self._get_connection()
        
        try:
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT first_trade_timestamp
                FROM {self.UPDATE_RECORD_TABLE}
                WHERE address = %s
            """, (address,))
            
            result = cursor.fetchone()
            
            if result and result[0]:
                return int(result[0])
            return None
            
        finally:
            conn.close()
    
    def check_data_exists(self, address: str, interval: str) -> Dict:
        """
        快速检查数据是否存在（从更新记录表查询）
        
        参数:
            address: 账户地址
            interval: 时间区间
        
        返回:
            {
                'exists': True/False,
                'last_update': 最后更新时间（毫秒时间戳，如果不存在则为None）
            }
        """
        if interval not in self.INTERVAL_TABLE_MAP:
            raise ValueError(f"不支持的时间区间: {interval}")
        
        conn = self._get_connection()
        
        try:
            cursor = conn.cursor()
            
            # 检查更新记录表是否存在
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (self.UPDATE_RECORD_TABLE,))
            
            if not cursor.fetchone()[0]:
                # 更新记录表不存在，降级到查询数据表
                table_name = self._get_table_name(interval)
                cursor.execute(sql.SQL("""
                    SELECT COUNT(*)
                    FROM {}
                    WHERE address = %s AND total_shares > 0
                """).format(sql.Identifier(table_name)), (address,))
                
                count = cursor.fetchone()[0]
                return {
                    'exists': count > 0,
                    'last_update': None
                }
            
            # 从更新记录表查询
            column_name = f"time_{interval}"
            cursor.execute(f"""
                SELECT EXTRACT(EPOCH FROM {column_name}) * 1000
                FROM {self.UPDATE_RECORD_TABLE}
                WHERE address = %s
            """, (address,))
            
            result = cursor.fetchone()
            
            if result and result[0]:
                # 有更新记录，说明有数据
                return {
                    'exists': True,
                    'last_update': int(result[0])
                }
            else:
                # 没有更新记录，说明没有数据
                return {
                    'exists': False,
                    'last_update': None
                }
            
        finally:
            conn.close()

