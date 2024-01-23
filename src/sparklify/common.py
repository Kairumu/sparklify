import os
import sys
import traceback

class io:

    class checker:
        def __init__(_self, _spark):
            _self.spark = _spark
        def check_exists(_self, _path):
            jvm = _self.spark._jvm
            jsc = _self.spark._jsc
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(
                jvm.java.net.URI.create(_path),
                jsc.hadoopConfiguration()
            )
            if fs.exists(jvm.org.apache.hadoop.fs.Path(_path)):
                return True
            return False
        @staticmethod
        def multipath_check(_path):
            return _path if isinstance(_path, list) else [_path]


    def __init__(_self, _spark, _base_path="hdfs://abc/user"):
        _self.spark = _spark
        _self.base_path = _base_path
        _self.read = _self.reader(_spark)
        _self.write = _self.writer()
        _self.consume = _self.consumer(_self.reader(_spark), _base_path)
        _self.produce = _self.producer(_base_path)


    class reader:
        def __init__(_self, _spark):
            _self.spark = _spark
        def orc(_self, _path, _schema=None):
            spark_read = _self.schema_check(_self.spark, _schema)
            return spark_read.orc(*checker.multipath_check(_path)) 
        def parquet(_self, _path, _schema=None):
            spark_read = _self.schema_check(_self.spark, _schema)
            return spark_read.parquet(*checker.multipath_check(_path)) 
        def json(_self, _path, _schema=None):
            spark_read = _self.schema_check(_self.spark, _schema)
            return spark_read.json(*checker.multipath_check(_path)) 
        def csv(_self, _path, _delimeter="\t", _header=False, _schema=None):
            spark_read = _self.schema_check(_self.spark, _schema)
            return spark_read.option("delimiter", _delimeter).option("header", _header).csv(*checker.multipath_check(_path))
        @staticmethod
        def schema_check(_spark, _schema):
            return _spark.read.schema(_schema) if _schema else _spark.read


    class writer:
        @staticmethod
        def orc(_df, _path, _overwrite=False):
            return (
                _df.write.mode("overwrite").orc(*checker.multipath_check(_path)) 
                if _overwrite else 
                _df.write.orc(*checker.multipath_check(_path)) 
            )
        @staticmethod
        def parquet(_df, _path, _overwrite=False):
            return (
                _df.write.mode("overwrite").parquet(_path) 
                if _overwrite else 
                _df.write.parquet(_path) 
            )
        @staticmethod
        def json(_df, _path, _overwrite=False):
            return (
                _df.write.mode("overwrite").json(_path) 
                if _overwrite else 
                _df.write.json(_path) 
            )
        @staticmethod
        def csv(_df, _path, _delimeter="\t", _overwrite=False):
            return (
                _df.write.mode("overwrite").option("delimeter", _delimeter).csv(_path) 
                if _overwrite else 
                _df.write.option("delimeter", _delimeter).csv(_path) 
            )


    class consumer:
        def __init__(_self, _reader, _base_path):
            _self.read = _reader
            _self.base_path = _base_path
        def orc(_self, _stage, _module, _target, _partition=datetime.now().strftime("%Y%m%d"), _schema=None):
            return _self.read.orc(f"{_self.base_path}/{_stage}/{_module}/{_target}/{_partition}", _schema)
        def parquet(_self, _stage, _module, _target, _partition=datetime.now().strftime("%Y%m%d"), _schema=None):
            return _self.read.parquet(f"{_self.base_path}/{_stage}/{_module}/{_target}/{_partition}", _schema)
        def json(_self, _stage, _module, _target, _partition=datetime.now().strftime("%Y%m%d"), _schema=None):
            return _self.read.json(f"{_self.base_path}/{_stage}/{_module}/{_target}/{_partition}", _schema)
        def csv(_self, _stage, _module, _target, _partition=datetime.now().strftime("%Y%m%d"), _delimeter="\t", _header=False, _schema=None):
            return _self.read.csv(f"{_self.base_path}/{_stage}/{_module}/{_target}/{_partition}", _delimeter, _header, _schema)


    class producer:
        def __init__(_self, _base_path):
            _self.base_path = _base_path
        def orc(_self, _df, _stage, _module, _target, _partition=datetime.now().strftime("%Y%m%d"), _overwrite=False):
            return writer.orc(_df, f"{_self.base_path}/{_stage}/{_module}/{_target}/{_partition}", _overwrite)
        def parquet(_self, _df, _stage, _module, _target, _partition=datetime.now().strftime("%Y%m%d"), _overwrite=False):
            return writer.parquet(_df, f"{_self.base_path}/{_stage}/{_module}/{_target}/{_partition}", _overwrite)
        def json(_self, _df, _stage, _module, _target, _partition=datetime.now().strftime("%Y%m%d"), _overwrite=False):
            return _self.read.json(_df, f"{_self.base_path}/{_stage}/{_module}/{_target}/{_partition}", _overwrite)
        def csv(_self, _df, _stage, _module, _target, _partition=datetime.now().strftime("%Y%m%d"), _delimeter="\t", _overwrite=False:
            return _self.read.csv(_df, f"{_self.base_path}/{_stage}/{_module}/{_target}/{_partition}", _delimeter, _overwrite)
