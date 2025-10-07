# import astroid
# from pylint.checkers import BaseChecker
# from pylint.lint import PyLinter
# import os

# class RepositoryRulesChecker(BaseChecker):
#     """Checker enforcing Repository rules & conventions"""

#     name = "repository-rules-checker"
#     priority = -1
#     msgs = {
#         "E9910": (
#             "Repository class '%s' must inherit from BaseAppRepository",
#             "repository-must-inherit-baseapp",
#             "All repositories must inherit from BaseAppRepository",
#         ),
#         "E9911": (
#             "Repository class '%s' must end with 'Repository'",
#             "repository-name-must-end-repository",
#             "All repository class names must end with 'Repository'",
#         ),
#         "E9912": (
#             "Repository file '%s' must end with '_repository.py'",
#             "repository-file-must-match-naming",
#             "Repository file must follow <model>_repository.py convention",
#         ),
#     }

#     def _is_repository_file(self, node: astroid.NodeNG) -> bool:
#         """Check if current file is *_repository.py (excluding baseapp_repository.py)"""
#         filename = node.root().file or ""
#         basename = os.path.basename(filename)
#         return basename.endswith("_repository.py") and basename != "baseapp_repository.py"

#     def visit_classdef(self, node: astroid.ClassDef):
#         if not self._is_repository_file(node):
#             return

#         # Must inherit from BaseAppRepository (generic allowed)
#         base_names = [b.as_string() for b in node.bases]
#         if not any(name.startswith("BaseAppRepository") for name in base_names):
#             self.add_message(
#                 "repository-must-inherit-baseapp", node=node, args=(node.name,)
#             )

#         # Must end with Repository
#         if not node.name.endswith("Repository"):
#             self.add_message(
#                 "repository-name-must-end-repository", node=node, args=(node.name,)
#             )

#     def visit_module(self, node: astroid.Module):
#         filename = node.file or ""
#         basename = os.path.basename(filename)

#         # Skip baseapp_repository.py
#         if basename == "baseapp_repository.py":
#             return

#         # Check repository file naming
#         if filename and not filename.endswith("_repository.py"):
#             self.add_message(
#                 "repository-file-must-match-naming", node=node, args=(filename,)
#             )


# def register(linter: PyLinter):
#     """Register the checker"""
#     linter.register_checker(RepositoryRulesChecker(linter))




import astroid
from pylint.checkers import BaseChecker
from pylint.lint import PyLinter
import os


class RepositoryRulesChecker(BaseChecker):
    """Checker enforcing Repository rules & conventions"""

    name = "repository-rules-checker"
    priority = -1
    msgs = {
        "E9940": (
            "Repository class '%s' must inherit from BaseAppRepository",
            "repository-must-inherit-baseapp",
            "All repositories must inherit from BaseAppRepository",
        ),
        "E9941": (
            "Repository class '%s' must end with 'Repository'",
            "repository-name-must-end-repository",
            "All repository class names must end with 'Repository'",
        ),
        "E9942": (
            "Repository file '%s' must end with '_repository.py'",
            "repository-file-must-match-naming",
            "Repository file must follow <model>_repository.py convention",
        ),
        "E9943": (
            "Function argument '%s' of type dict must be named '<model>_data'",
            "repository-arg-dict-naming",
            "Dict arguments must follow naming convention based on model name.",
        ),
        "E9944": (
            "Function argument '%s' of type UUID must end with '_id'",
            "repository-arg-uuid-naming",
            "UUID arguments must end with '_id'.",
        ),
        "E9945": (
            "Repository method '%s' must not use Optional for return types",
            "repository-no-optional-return",
            "Repository methods must return model objects on success, errors must be raised as exceptions.",
        ),
        "E9946": (
            "Repository method '%s' must not return None",
            "repository-no-none-return",
            "Repository methods must return something (not None) on success, errors must be raised as exceptions.",
        ),
    }

    def _is_repository_file(self, node: astroid.NodeNG) -> bool:
        """Check if current file is *_repository.py (excluding baseapp_repository.py)"""
        filename = node.root().file or ""
        basename = os.path.basename(filename)
        return basename.endswith("_repository.py") and basename != "baseapp_repository.py"

    def _get_model_name(self, filename: str) -> str:
        """Extract model name from repository filename (workspace_repository.py → workspace)"""
        basename = os.path.basename(filename)
        if basename.endswith("_repository.py"):
            return basename.replace("_repository.py", "")
        return ""

    def visit_classdef(self, node: astroid.ClassDef):
        if not self._is_repository_file(node):
            return

        # Must inherit from BaseAppRepository (generic allowed)
        base_names = [b.as_string() for b in node.bases]
        if not any(name.startswith("BaseAppRepository") for name in base_names):
            self.add_message(
                "repository-must-inherit-baseapp", node=node, args=(node.name,)
            )

        # Must end with Repository
        if not node.name.endswith("Repository"):
            self.add_message(
                "repository-name-must-end-repository", node=node, args=(node.name,)
            )

    def visit_module(self, node: astroid.Module):
        filename = node.file or ""
        basename = os.path.basename(filename)

        # Only apply to files in repository directory
        if not filename or "repository" not in filename.replace("\\", "/").split("/"):
            return

        # Skip baseapp_repository.py
        if basename == "baseapp_repository.py":
            return

        # Check repository file naming
        if filename and not filename.endswith("_repository.py"):
            self.add_message(
                "repository-file-must-match-naming", node=node, args=(filename,)
            )

    def visit_functiondef(self, node: astroid.FunctionDef):
        self._check_function_arguments(node)

    def visit_asyncfunctiondef(self, node: astroid.AsyncFunctionDef):
        self._check_function_arguments(node)

    def _check_function_arguments(self, node: astroid.FunctionDef | astroid.AsyncFunctionDef):
        if not self._is_repository_file(node):
            return

        filename = node.root().file or ""
        model_name = self._get_model_name(filename)

        # Check return type for Optional usage and None returns
        if node.returns:
            return_ann_str = node.returns.as_string()
            if "Optional" in return_ann_str or "Union" in return_ann_str:
                self.add_message(
                    "repository-no-optional-return", node=node, args=(node.name,)
                )
            elif return_ann_str in ("None", "NoneType"):
                self.add_message(
                    "repository-no-none-return", node=node, args=(node.name,)
                )

        # Get annotations from node.args.annotations
        annotations = node.args.annotations
        for i, arg in enumerate(node.args.args):
            if arg.name == "self":
                continue

            # Check if there's an annotation for this argument
            if i >= len(annotations) or annotations[i] is None:
                continue  # no type annotation, skip

            ann_str = annotations[i].as_string()

            # Rule 1: dict → must be <model>_data
            if ann_str in ("dict", "Dict", "typing.Dict", "Dict[str, Any]", "typing.Dict[str, Any]"):
                expected_name = f"{model_name}_data"
                if arg.name != expected_name:
                    self.add_message(
                        "repository-arg-dict-naming", node=arg, args=(arg.name,)
                    )

            # Rule 2: UUID → must end with _id
            if ann_str in ("UUID", "uuid.UUID"):
                if not arg.name.endswith("_id"):
                    self.add_message(
                        "repository-arg-uuid-naming", node=arg, args=(arg.name,)
                    )


def register(linter: PyLinter):
    """Register the checker"""
    linter.register_checker(RepositoryRulesChecker(linter))
