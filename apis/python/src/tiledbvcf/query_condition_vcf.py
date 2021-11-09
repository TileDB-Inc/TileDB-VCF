import ast
import numpy as np

import tiledb
import tiledb.main as qc
from tiledb.main import PyQueryCondition


class QueryConditionVCF(tiledb.QueryCondition):
    def __capsule__(self):
        if self._c_obj is None:
            raise tiledb.TileDBError(
                "internal error: cannot create capsule for uninitialized _c_obj!"
            )
        return self._c_obj.get_capsule()

    def init_query_condition(self, query_attrs):
        self._query_attrs = query_attrs
        self._c_obj = self.visit(self.tree.body[0])

        if not isinstance(self._c_obj, tiledb.main.PyQueryCondition):
            raise tiledb.TileDBError(
                "Malformed query condition statement. A query condition must "
                "be made up of one or more Boolean expressions."
            )

    def aux_visit_Compare(self, field_name, op, val):
        AST_TO_TILEDB = {
            ast.Gt: qc.TILEDB_GT,
            ast.GtE: qc.TILEDB_GE,
            ast.Lt: qc.TILEDB_LT,
            ast.LtE: qc.TILEDB_LE,
            ast.Eq: qc.TILEDB_EQ,
            ast.NotEq: qc.TILEDB_NE,
        }

        try:
            op = AST_TO_TILEDB[type(op)]
        except KeyError:
            raise tiledb.TileDBError("Unsupported comparison operator.")

        if not isinstance(field_name, ast.Name):
            REVERSE_OP = {
                qc.TILEDB_GT: qc.TILEDB_LT,
                qc.TILEDB_GE: qc.TILEDB_LE,
                qc.TILEDB_LT: qc.TILEDB_GT,
                qc.TILEDB_LE: qc.TILEDB_GE,
                qc.TILEDB_EQ: qc.TILEDB_EQ,
                qc.TILEDB_NE: qc.TILEDB_NE,
            }

            op = REVERSE_OP[op]
            field_name, val = val, field_name

        if isinstance(field_name, ast.Name):
            field_name = field_name.id
        else:
            raise tiledb.TileDBError("Incorrect type for attribute name.")

        if isinstance(val, ast.Constant):
            sign = val.sign if hasattr(val, "sign") else 1
            val = val.value * sign
        elif isinstance(val, ast.Num):
            # deprecated in 3.8
            sign = val.sign if hasattr(val, "sign") else 1
            val = val.n * sign
        elif isinstance(val, ast.Str) or isinstance(val, ast.Bytes):
            # deprecated in 3.8
            val = val.s
        else:
            raise tiledb.TileDBError(
                f"Incorrect type for comparison value: {ast.dump(val)}"
            )

        if field_name not in self._query_attrs:
            raise tiledb.TileDBError(
                f"Attribute `{field_name}` given to filter in query's `attr_cond` "
                "arg but not found in `attr` arg."
            )

        field_name_to_att = {
            "contig": "contig",
            "pos_end": "end_pos",
            "pos_start": "real_start_pos",
            "alleles": "alleles",
            "id": "id",
            "filters": "filters",
            "qual": "qual",
            "fmt": "fmt",
            "info": "info",
        }

        if field_name not in field_name_to_att:
            raise tiledb.TileDBError(f"`{field_name}` is not a valid VCF attribute.")
        else:
            att = field_name_to_att[field_name]

        att_to_dtype = {
            "contig": np.dtype("S0"),
            "end_pos": np.dtype("uint32"),
            "real_start_pos": np.dtype("uint32"),
            "alleles": np.dtype("S0"),
            "id": np.dtype("S0"),
            "filters": np.dtype("uint32"),
            "qual": np.dtype("float"),
            "fmt": np.dtype("U1"),
            "info": np.dtype("U1"),
        }

        if att not in att_to_dtype:
            if att.startswith("info") or att.startswith("fmt"):
                dtype = np.dtype("U1")
            else:
                raise tiledb.TileDBError(f"`{att}` is not a valid VCF attribute.")
        else:
            dtype = att_to_dtype[att]

        if dtype.kind in "SUa":
            dtype_name = "string"
        else:
            try:
                # this prevents numeric strings ("1", '123.32') from getting
                # casted to numeric types
                if isinstance(val, str):
                    raise tiledb.TileDBError(
                        f"Type mismatch between attribute `{att}` and value `{val}`."
                    )

                cast = getattr(np, dtype.name)
                val = cast(val)
                dtype_name = dtype.name
            except ValueError:
                raise tiledb.TileDBError(
                    f"Type mismatch between attribute `{att}` and value `{val}`."
                )

        result = PyQueryCondition(self._ctx)

        if not hasattr(result, f"init_{dtype_name}"):
            raise tiledb.TileDBError(
                f"PyQueryCondition's `init_{dtype_name}` not found."
            )

        init_qc = getattr(result, f"init_{dtype_name}")

        try:
            init_qc(att, val, op)
        except tiledb.TileDBError as e:
            raise tiledb.TileDBError(e)

        return result
