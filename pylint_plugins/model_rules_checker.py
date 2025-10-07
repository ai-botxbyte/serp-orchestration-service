import astroid
from pylint.checkers import BaseChecker
from pylint.lint import PyLinter
 
 
class ModelRulesChecker(BaseChecker):
    """Checker that enforces SQLAlchemy Model Rules & Conventions"""
 
    name = "model-rules-checker"
    priority = -1
    msgs = {
        "E9910": (
            "Model class '%s' must inherit from BaseAppModel",
            "model-must-inherit-baseappmodel",
            "All models must inherit from BaseAppModel (except BaseAppModel itself).",
        ),
        "E9911": (
            "Model class '%s' must end with 'Model'",
            "model-name-must-end-with-model",
            "All model classes must end with 'Model'.",
        ),
        "E9912": (
            "Primary key '%s' must be UUID(as_uuid=True) with default=uuid.uuid4",
            "primary-key-uuid-required",
            "Primary key fields must be UUID(as_uuid=True) with uuid.uuid4 as default.",
        ),
        "E9913": (
            "Column '%s' must be String with a length defined and <= 255",
            "string-column-rules",
            "All String columns must explicitly define a length not greater than 255.",
        ),
        "E9914": (
            "Boolean field '%s' must have default and nullable=False",
            "boolean-rules",
            "Boolean fields must always define a default and set nullable=False.",
        ),
        "E9915": (
            "Status field '%s' must be String(20) with default='created'",
            "status-field-rules",
            "Status fields must be String(20) with default='created'.",
        ),
        "E9917": (
            "DateTime column '%s' must use timezone=True",
            "datetime-timezone-required",
            "All DateTime columns must include timezone=True.",
        ),
        "E9918": (
            "Base model '%s' must have __abstract__ = True",
            "must-have-abstract-attr",
            "Abstract base classes must define __abstract__ = True.",
        ),
        "E9919": (
            "Model class '%s' must define __tablename__",
            "missing-tablename",
            "All concrete SQLAlchemy models must define __tablename__.",
        ),
    }
 
 
    def _is_model_file(self, node: astroid.NodeNG) -> bool:
        """Check if filename ends with _model.py"""
        filename = node.root().file or ""
        return filename.endswith("_model.py")
 
    def visit_classdef(self, node: astroid.ClassDef):
        if not self._is_model_file(node):
            return
 
        # Allow BaseAppModel itself (the abstract base class)
        if node.name == "BaseAppModel":
            # ---- New Rule: Must have __abstract__ = True ----
            has_abstract = any(
                isinstance(stmt, astroid.Assign)
                and stmt.targets[0].as_string() == "__abstract__"
                and stmt.value.as_string() == "True"
                for stmt in node.body
            )
            if not has_abstract:
                self.add_message(
                    "must-have-abstract-attr",
                    node=node,
                    args=(node.name,),
                )
            return
   
        # ---- New Rule: must have __tablename__ ----
        has_tablename = any(
            isinstance(stmt, astroid.Assign)
            and stmt.targets[0].as_string() == "__tablename__"
            for stmt in node.body
        )
        if not has_tablename:
            self.add_message("missing-tablename", node=node, args=(node.name,))
 
        # Rule: must inherit BaseAppModel
        base_names = [b.as_string() for b in node.bases]
        if "BaseAppModel" not in base_names:
            self.add_message("model-must-inherit-baseappmodel", node=node, args=(node.name,))
 
        # Rule: class name must end with Model
        if not node.name.endswith("Model"):
            self.add_message("model-name-must-end-with-model", node=node, args=(node.name,))
 
    def visit_assign(self, node: astroid.Assign):
        if not self._is_model_file(node):
            return
 
        if not isinstance(node.value, astroid.Call):
            return
        if not hasattr(node.value.func, "as_string"):
            return
 
        func_name = node.value.func.as_string()
        if func_name != "Column":
            return
 
        col_args = [a.as_string() for a in node.value.args] if node.value.args else []
        col_kwargs = {kw.arg: kw.value.as_string() for kw in node.value.keywords if kw.arg}
        col_name = node.targets[0].as_string()
 
        # ---- Primary Key Rule ----
        if "primary_key=True" in col_args or col_kwargs.get("primary_key") == "True":
            if "UUID(as_uuid=True)" not in col_args or col_kwargs.get("default") != "uuid.uuid4":
                self.add_message("primary-key-uuid-required", node=node, args=(col_name,))
 
        # ---- String Column Rule ----
        if col_args and col_args[0].startswith("String"):
            length = None
            if "(" in col_args[0] and ")" in col_args[0]:
                try:
                    length = int(col_args[0].replace("String(", "").replace(")", ""))
                except ValueError:
                    length = None
            if length is None or length > 255:
                self.add_message("string-column-rules", node=node, args=(col_name,))
 
        # ---- Boolean Rule ----
        if any("Boolean" in arg for arg in col_args):
            if not (
                col_kwargs.get("default") is not None
                and col_kwargs.get("nullable") == "False"
            ):
                self.add_message("boolean-rules", node=node, args=(col_name,))
 
        # ---- Status Rule ----
        if col_name == "status":
            if not ("String(20)" in col_args and col_kwargs.get("default") == "'created'"):
                self.add_message("status-field-rules", node=node, args=(col_name,))
 
 
        # ---- DateTime Rule ----
        if col_args and "DateTime" in col_args[0]:
            # Check for timezone=True
            if "timezone=True" not in node.value.as_string():
                self.add_message("datetime-timezone-required", node=node, args=(col_name,))
 
 
def register(linter: PyLinter):
    """Register the checker with pylint"""
    linter.register_checker(ModelRulesChecker(linter))
 
 
 