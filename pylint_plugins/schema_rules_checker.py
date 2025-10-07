# pylint_plugins/schema_rules_checker.py
import astroid
from pylint.checkers import BaseChecker
from pylint.lint import PyLinter
import os


class SchemaRulesChecker(BaseChecker):
    """Checker enforcing Schema rules & conventions based on your guidelines."""

    name = "schema-rules-checker"
    priority = -1
    msgs = {
        "E9931": (
            "Schema class '%s' must end with 'Schema'",
            "schema-name-must-end-with-schema",
            "All schema class names must end with 'Schema'.",
        ),
        "E9932": (
            "Enum field '%s' must use Enum type with descriptive name",
            "enum-field-rule",
            "All status-like fields must use Enum with descriptive names and docstrings.",
        ),
        "E9933": (
            "Boolean field '%s' must start with 'is_' and have default=True",
            "boolean-field-rule",
            "Boolean fields must start with 'is_' and define default=True to indicate flags.",
        ),
        "E9934": (
            "Field '%s' must define type, optionality, and description using Field()",
            "field-validation-rule",
            "All schema fields must use Field() with type, optionality, default, and description.",
        ),
    }

    # ---------------- Utility Methods ----------------
    def _is_schema_class(self, node):
        """Check if the class is a schema class by verifying it inherits from BaseAppSchema."""
        if not isinstance(node, astroid.ClassDef):
            return False
        
        # Get base class names
        base_names = [b.as_string() for b in node.bases]
        
        # Check if it inherits from BaseAppSchema
        return "BaseAppSchema" in base_names

    def _get_base_names(self, node):
        return [b.as_string() for b in node.bases]

    def _get_field_assignments(self, node):
        return [n for n in node.body if isinstance(n, (astroid.Assign, astroid.AnnAssign))]

    # ---------------- Visitors ----------------
    def visit_classdef(self, node: astroid.ClassDef):
        # Only apply to files ending with _schema.py
        if not node.root().file or not node.root().file.endswith("_schema.py"):
            return
            
        # Skip base schema file
        if node.root().file and os.path.basename(node.root().file) == "baseapp_schema.py":
            return

        if not self._is_schema_class(node):
            return

        class_name = node.name
        base_names = self._get_base_names(node)


        # ---- Rule: Class name must end with 'Schema' ----
        if not class_name.endswith("Schema"):
            self.add_message("schema-name-must-end-with-schema", node=node, args=(class_name,))

        # ---- Field Checks ----
        for assign in self._get_field_assignments(node):
            if isinstance(assign, astroid.Assign):
                field_name = assign.targets[0].as_string()
                field_value = assign.value.as_string() if hasattr(assign.value, "as_string") else ""
                field_annotation = ""
            elif isinstance(assign, astroid.AnnAssign):
                field_name = assign.target.as_string()
                field_annotation = assign.annotation.as_string() if assign.annotation else ""
                field_value = assign.value.as_string() if assign.value else None
            else:
                continue

            # Boolean fields must start with 'is_' and have default True
            if field_annotation == "bool":
                if not field_name.startswith("is_"):
                    self.add_message("boolean-field-rule", node=assign, args=(field_name,))
                elif field_value and "default=True" not in field_value and field_value != "True":
                    # Check if it has default=True in Field() or is just True
                    self.add_message("boolean-field-rule", node=assign, args=(field_name,))
                elif isinstance(assign, astroid.AnnAssign) and not field_value:
                    # For AnnAssign without default value, it should use Field()
                    self.add_message("boolean-field-rule", node=assign, args=(field_name,))
                # Skip field validation check for boolean fields as they're handled above
                continue

            # Enum usage (check for Enum in annotation)
            if "Enum" in field_annotation:
                # Enforce PascalCase naming
                if not field_name[0].isupper():
                    self.add_message("enum-field-rule", node=assign, args=(field_name,))

            # Field validation must use Field() (exclude special fields like model_config)
            if field_name not in ["model_config", "ConfigDict"]:
                if field_value and "Field(" not in field_value:
                    self.add_message("field-validation-rule", node=assign, args=(field_name,))
                elif isinstance(assign, astroid.AnnAssign) and not field_value and not field_annotation.startswith("Optional["):
                    # For AnnAssign without Field(), check if it's a simple type annotation
                    if field_annotation in ["str", "int", "bool", "float", "UUID", "datetime"]:
                        self.add_message("field-validation-rule", node=assign, args=(field_name,))



def register(linter: PyLinter):
    """Register the checker with pylint"""
    linter.register_checker(SchemaRulesChecker(linter))
