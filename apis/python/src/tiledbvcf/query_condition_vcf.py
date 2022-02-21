import ast
import numpy as np

import tiledb
import tiledb.main as qc
from tiledb.main import PyQueryCondition

import ast
from dataclasses import dataclass, field
import numpy as np
from typing import Any, Callable, List, Tuple, Type, Union

import tiledb
import tiledb.main as qc
from tiledb.main import PyQueryCondition

"""
A high level wrapper around the Pybind11 query_condition.cc implementation for
filtering query results on attribute values.
"""

QueryConditionNodeElem = Union[
    ast.Name, ast.Constant, ast.Call, ast.Num, ast.Str, ast.Bytes
]


@dataclass
class QueryConditionVCF:
    """
    Class representing a TileDB query condition object for attribute filtering
    pushdown. Set the query condition with a string representing an expression
    as defined by the grammar below. A more straight forward example of usage is
    given beneath.

    BNF
    ---
    A query condition is made up of one or more Boolean expressions. Multiple
    Boolean expressions are chained together with Boolean operators.

        query_cond ::= bool_expr | bool_expr bool_op query_cond

    A Boolean expression contains a comparison operator. The operator works on a
    TileDB attribute name and value.

        bool_expr ::= attr compare_op val | val compare_op attr | val compare_op attr compare_op val

    "and" and "&" are the only Boolean operators supported at the moment. We
    intend to support "or" and "not" in future releases.

        bool_op ::= and | &

    All comparison operators are supported.

        compare_op ::= < | > | <= | >= | == | !=

    TileDB attribute names are Python valid variables or a attr() casted string.

        attr ::= <variable> | attr(<str>)

    Values are any Python-valid number or string. They may also be casted with val().

        val ::= <num> | <str> | val(val)

    Example
    -------
    with tiledb.open(uri, mode="r") as A:
        # select cells where the attribute values for foo are less than 5
        # and bar equal to string asdf.
        qc = QueryCondition("foo > 5 and 'asdf' == attr('b a r') and baz <= val(1.0)")
        A.query(attr_cond=qc)
    """

    expression: str
    ctx: tiledb.Ctx = field(default_factory=tiledb.default_ctx, repr=False)
    tree: ast.Expression = field(init=False, repr=False)
    c_obj: PyQueryCondition = field(init=False, repr=False)

    def __post_init__(self):
        try:
            self.tree = ast.parse(self.expression, mode="eval")
        except:
            raise tiledb.TileDBError(
                "Could not parse the given QueryCondition statement: "
                f"{self.expression}"
            )

        if not self.tree:
            raise tiledb.TileDBError(
                "The query condition statement could not be parsed properly. "
                "(Is this an empty expression?)"
            )

    def init_query_condition(self, query_attrs: List[str]):
        qctree = QueryConditionTreeVCF(self.ctx, query_attrs)
        self.c_obj = qctree.visit(self.tree.body)

        if not isinstance(self.c_obj, PyQueryCondition):
            raise tiledb.TileDBError(
                "Malformed query condition statement. A query condition must "
                "be made up of one or more Boolean expressions."
            )


@dataclass
class QueryConditionTreeVCF(ast.NodeVisitor):
    ctx: tiledb.Ctx
    query_attrs: List[str]

    def visit_Gt(self, node):
        return qc.TILEDB_GT

    def visit_GtE(self, node):
        return qc.TILEDB_GE

    def visit_Lt(self, node):
        return qc.TILEDB_LT

    def visit_LtE(self, node):
        return qc.TILEDB_LE

    def visit_Eq(self, node):
        return qc.TILEDB_EQ

    def visit_NotEq(self, node):
        return qc.TILEDB_NE

    def visit_BitAnd(self, node):
        return qc.TILEDB_AND

    def visit_And(self, node):
        return qc.TILEDB_AND

    def visit_Compare(self, node: Type[ast.Compare]) -> PyQueryCondition:
        result = self.aux_visit_Compare(
            self.visit(node.left),
            self.visit(node.ops[0]),
            self.visit(node.comparators[0]),
        )

        for lhs, op, rhs in zip(
            node.comparators[:-1], node.ops[1:], node.comparators[1:]
        ):
            value = self.aux_visit_Compare(
                self.visit(lhs), self.visit(op), self.visit(rhs)
            )
            result = result.combine(value, qc.TILEDB_AND)

        return result

    def aux_visit_Compare(
        self,
        lhs: QueryConditionNodeElem,
        op_node: qc.tiledb_query_condition_op_t,
        rhs: QueryConditionNodeElem,
    ) -> PyQueryCondition:
        att, val, op = self.order_nodes(lhs, rhs, op_node)

        att = self.get_att_from_node(att)
        val = self.get_val_from_node(val)

        att_to_dtype = {
            "contig": "string",
            "end_pos": "int32",
            "real_start_pos": "int32",
            "alleles": "string",
            "id": "string",
            "filters": "uint32",
            "qual": "float32",
            "fmt": "string",
            "info": "string",
        }

        if att not in att_to_dtype:
            if att.startswith("info") or att.startswith("fmt"):
                dtype = "string"
            else:
                raise tiledb.TileDBError(f"`{att}` is not a valid VCF attribute.")
        else:
            dtype = att_to_dtype[att]

        val = self.cast_val_to_dtype(val, dtype)

        pyqc = PyQueryCondition(self.ctx)

        try:
            self.init_pyqc(pyqc, dtype)(att, val, op)
        except tiledb.TileDBError as e:
            raise tiledb.TileDBError(e)

        return pyqc

    def is_att_node(self, att: QueryConditionNodeElem) -> bool:
        if isinstance(att, ast.Call):
            if not isinstance(att.func, ast.Name):
                raise tiledb.TileDBError(f"Unrecognized expression {att.func}.")

            if att.func.id != "attr":
                return False

            return (
                isinstance(att.args[0], ast.Constant)
                or isinstance(att.args[0], ast.Str)
                or isinstance(att.args[0], ast.Bytes)
            )

        return isinstance(att, ast.Name)

    def order_nodes(
        self,
        att: QueryConditionNodeElem,
        val: QueryConditionNodeElem,
        op: qc.tiledb_query_condition_op_t,
    ) -> Tuple[
        QueryConditionNodeElem,
        QueryConditionNodeElem,
        qc.tiledb_query_condition_op_t,
    ]:
        if not self.is_att_node(att):
            REVERSE_OP = {
                qc.TILEDB_GT: qc.TILEDB_LT,
                qc.TILEDB_GE: qc.TILEDB_LE,
                qc.TILEDB_LT: qc.TILEDB_GT,
                qc.TILEDB_LE: qc.TILEDB_GE,
                qc.TILEDB_EQ: qc.TILEDB_EQ,
                qc.TILEDB_NE: qc.TILEDB_NE,
            }

            op = REVERSE_OP[op]
            att, val = val, att

        return att, val, op

    def get_att_from_node(self, node: QueryConditionNodeElem) -> Any:
        if self.is_att_node(node):
            att_node = node

            if isinstance(att_node, ast.Call):
                if not isinstance(att_node.func, ast.Name):
                    raise tiledb.TileDBError(
                        f"Unrecognized expression {att_node.func}."
                    )
                att_node = att_node.args[0]

            if isinstance(att_node, ast.Name):
                field_name = att_node.id
            elif isinstance(att_node, ast.Constant):
                field_name = att_node.value
            elif isinstance(att_node, ast.Str) or isinstance(att_node, ast.Bytes):
                # deprecated in 3.8
                field_name = att_node.s
            else:
                raise tiledb.TileDBError(
                    f"Incorrect type for attribute name: {ast.dump(att_node)}"
                )
        else:
            raise tiledb.TileDBError(
                f"Incorrect type for attribute name: {ast.dump(node)}"
            )

        if field_name not in self.query_attrs:
            raise tiledb.TileDBError(
                f"Attribute `{field_name}` given to filter in query's `attr_cond` "
                "arg but not found in `attr` arg."
            )

        field_name_to_att = {
            "sample_name": "sample",
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
        att = field_name_to_att[field_name]

        return att

    def get_val_from_node(self, node: QueryConditionNodeElem) -> Any:
        val_node = node

        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name):
                raise tiledb.TileDBError(f"Unrecognized expression {node.func}.")

            if node.func.id == "val":
                val_node = node.args[0]
            else:
                raise tiledb.TileDBError(
                    f"Incorrect type for cast value: {node.func.id}"
                )

        if isinstance(val_node, ast.Constant):
            val = val_node.value
        elif isinstance(val_node, ast.Num):
            # deprecated in 3.8
            val = val_node.n
        elif isinstance(val_node, ast.Str) or isinstance(val_node, ast.Bytes):
            # deprecated in 3.8
            val = val_node.s
        else:
            raise tiledb.TileDBError(
                f"Incorrect type for comparison value: {ast.dump(val_node)}"
            )

        return val

    def cast_val_to_dtype(
        self, val: Union[str, int, float, bytes], dtype: str
    ) -> Union[str, int, float, bytes]:
        if dtype != "string":
            try:
                # this prevents numeric strings ("1", '123.32') from getting
                # casted to numeric types
                if isinstance(val, str):
                    raise tiledb.TileDBError(f"Cannot cast `{val}` to {dtype}.")
                cast = getattr(np, dtype)
                val = cast(val)
            except ValueError:
                raise tiledb.TileDBError(f"Cannot cast `{val}` to {dtype}.")

        return val

    def init_pyqc(self, pyqc: PyQueryCondition, dtype: str) -> Callable:
        init_fn_name = f"init_{dtype}"

        if not hasattr(pyqc, init_fn_name):
            raise tiledb.TileDBError(f"PyQueryCondition.{init_fn_name}() not found.")

        return getattr(pyqc, init_fn_name)

    def visit_BinOp(self, node: ast.BinOp) -> PyQueryCondition:
        try:
            op = self.visit(node.op)
        except KeyError:
            raise tiledb.TileDBError(
                f"Unsupported binary operator: {ast.dump(node.op)}. Only & is currently supported."
            )

        result = self.visit(node.left)
        rhs = node.right[1:] if isinstance(node.right, list) else [node.right]
        for value in rhs:
            result = result.combine(self.visit(value), op)

        return result

    def visit_BoolOp(self, node: ast.BoolOp) -> PyQueryCondition:
        try:
            op = self.visit(node.op)
        except KeyError:
            raise tiledb.TileDBError(
                f"Unsupported Boolean operator: {ast.dump(node.op)}. "
                'Only "and" is currently supported.'
            )

        result = self.visit(node.values[0])
        for value in node.values[1:]:
            result = result.combine(self.visit(value), op)

        return result

    def visit_Call(self, node: ast.Call) -> ast.Call:
        if not isinstance(node.func, ast.Name):
            raise tiledb.TileDBError(f"Unrecognized expression {node.func}.")

        if node.func.id not in ["attr", "val"]:
            raise tiledb.TileDBError(f"Valid casts are attr() or val().")

        if len(node.args) != 1:
            raise tiledb.TileDBError(
                f"Exactly one argument must be provided to {node.func.id}()."
            )

        return node

    def visit_Name(self, node: ast.Name) -> ast.Name:
        return node

    def visit_Constant(self, node: ast.Constant) -> ast.Constant:
        return node

    def visit_UnaryOp(self, node: ast.UnaryOp, sign: int = 1):
        if isinstance(node.op, ast.UAdd):
            sign *= 1
        elif isinstance(node.op, ast.USub):
            sign *= -1
        else:
            raise tiledb.TileDBError(f"Unsupported UnaryOp type. Saw {ast.dump(node)}.")

        if isinstance(node.operand, ast.UnaryOp):
            return self.visit_UnaryOp(node.operand, sign)
        else:
            if isinstance(node.operand, ast.Constant):
                node.operand.value *= sign
            elif isinstance(node.operand, ast.Num):
                node.operand.n *= sign
            else:
                raise tiledb.TileDBError(
                    f"Unexpected node type following UnaryOp. Saw {ast.dump(node)}."
                )

            return node.operand

    def visit_Num(self, node: ast.Num) -> ast.Num:
        # deprecated in 3.8
        return node

    def visit_Str(self, node: ast.Str) -> ast.Str:
        # deprecated in 3.8
        return node

    def visit_Bytes(self, node: ast.Bytes) -> ast.Bytes:
        # deprecated in 3.8
        return node


# class QueryConditionVCF(tiledb.QueryCondition):
#     def __capsule__(self):
#         if self._c_obj is None:
#             raise tiledb.TileDBError(
#                 "internal error: cannot create capsule for uninitialized _c_obj!"
#             )
#         return self._c_obj.get_capsule()

#     def init_query_condition(self, query_attrs):
#         self._query_attrs = query_attrs
#         self._c_obj = self.visit(self.tree.body[0])

#         if not isinstance(self._c_obj, tiledb.main.PyQueryCondition):
#             raise tiledb.TileDBError(
#                 "Malformed query condition statement. A query condition must "
#                 "be made up of one or more Boolean expressions."
#             )

#     def aux_visit_Compare(self, field_name, op, val):
#         AST_TO_TILEDB = {
#             ast.Gt: qc.TILEDB_GT,
#             ast.GtE: qc.TILEDB_GE,
#             ast.Lt: qc.TILEDB_LT,
#             ast.LtE: qc.TILEDB_LE,
#             ast.Eq: qc.TILEDB_EQ,
#             ast.NotEq: qc.TILEDB_NE,
#         }

#         try:
#             op = AST_TO_TILEDB[type(op)]
#         except KeyError:
#             raise tiledb.TileDBError("Unsupported comparison operator.")

#         if not isinstance(field_name, ast.Name):
#             REVERSE_OP = {
#                 qc.TILEDB_GT: qc.TILEDB_LT,
#                 qc.TILEDB_GE: qc.TILEDB_LE,
#                 qc.TILEDB_LT: qc.TILEDB_GT,
#                 qc.TILEDB_LE: qc.TILEDB_GE,
#                 qc.TILEDB_EQ: qc.TILEDB_EQ,
#                 qc.TILEDB_NE: qc.TILEDB_NE,
#             }

#             op = REVERSE_OP[op]
#             field_name, val = val, field_name

#         if isinstance(field_name, ast.Name):
#             field_name = field_name.id
#         else:
#             raise tiledb.TileDBError("Incorrect type for attribute name.")

#         if isinstance(val, ast.Constant):
#             sign = val.sign if hasattr(val, "sign") else 1
#             val = val.value * sign
#         elif isinstance(val, ast.Num):
#             # deprecated in 3.8
#             sign = val.sign if hasattr(val, "sign") else 1
#             val = val.n * sign
#         elif isinstance(val, ast.Str) or isinstance(val, ast.Bytes):
#             # deprecated in 3.8
#             val = val.s
#         else:
#             raise tiledb.TileDBError(
#                 f"Incorrect type for comparison value: {ast.dump(val)}"
#             )

#         if field_name not in self._query_attrs:
#             raise tiledb.TileDBError(
#                 f"Attribute `{field_name}` given to filter in query's `attr_cond` "
#                 "arg but not found in `attr` arg."
#             )

#         field_name_to_att = {
#             "contig": "contig",
#             "pos_end": "end_pos",
#             "pos_start": "real_start_pos",
#             "alleles": "alleles",
#             "id": "id",
#             "filters": "filters",
#             "qual": "qual",
#             "fmt": "fmt",
#             "info": "info",
#         }

#         if field_name not in field_name_to_att:
#             raise tiledb.TileDBError(f"`{field_name}` is not a valid VCF attribute.")
#         else:
#             att = field_name_to_att[field_name]

#         att_to_dtype = {
#             "contig": np.dtype("S0"),
#             "end_pos": np.dtype("uint32"),
#             "real_start_pos": np.dtype("uint32"),
#             "alleles": np.dtype("S0"),
#             "id": np.dtype("S0"),
#             "filters": np.dtype("uint32"),
#             "qual": np.dtype("float"),
#             "fmt": np.dtype("U1"),
#             "info": np.dtype("U1"),
#         }

#         if att not in att_to_dtype:
#             if att.startswith("info") or att.startswith("fmt"):
#                 dtype = np.dtype("U1")
#             else:
#                 raise tiledb.TileDBError(f"`{att}` is not a valid VCF attribute.")
#         else:
#             dtype = att_to_dtype[att]

#         if dtype.kind in "SUa":
#             dtype_name = "string"
#         else:
#             try:
#                 # this prevents numeric strings ("1", '123.32') from getting
#                 # casted to numeric types
#                 if isinstance(val, str):
#                     raise tiledb.TileDBError(
#                         f"Type mismatch between attribute `{att}` and value `{val}`."
#                     )

#                 cast = getattr(np, dtype.name)
#                 val = cast(val)
#                 dtype_name = dtype.name
#             except ValueError:
#                 raise tiledb.TileDBError(
#                     f"Type mismatch between attribute `{att}` and value `{val}`."
#                 )

#         result = PyQueryCondition(self._ctx)

#         if not hasattr(result, f"init_{dtype_name}"):
#             raise tiledb.TileDBError(
#                 f"PyQueryCondition's `init_{dtype_name}` not found."
#             )

#         init_qc = getattr(result, f"init_{dtype_name}")

#         try:
#             init_qc(att, val, op)
#         except tiledb.TileDBError as e:
#             raise tiledb.TileDBError(e)

#         return result
