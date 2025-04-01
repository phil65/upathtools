"""Configuration models for filesystem implementations."""

from __future__ import annotations

from typing import Any, ClassVar, Literal

import fsspec
from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict, Field
from upath import UPath


# Define filesystem categories as literals
FilesystemCategoryType = Literal["base", "archive", "transform", "aggregation", "wrapper"]


class FileSystemConfig(BaseModel):
    """Base configuration for filesystem implementations."""

    model_config = ConfigDict(extra="allow", use_attribute_docstrings=True)

    fs_type: str = Field(init=False)
    """Type of filesystem"""

    target_path: str | None = None
    """Path for target filesystem (for archive-type filesystems)"""

    root_path: str | None = None
    """Root directory to restrict filesystem access to (applies dir:: modifier)"""

    layers: list[FileSystemConfig] = Field(default_factory=list)
    """Optional list of nested filesystem configurations in order from innermost to outermost"""  # noqa: E501

    _category: ClassVar[FilesystemCategoryType] = "base"
    """Classification of the filesystem type"""

    @property
    def category(self) -> FilesystemCategoryType:
        """Get the category of this filesystem."""
        return self._category

    @property
    def is_typically_layered(self) -> bool:
        """Whether this filesystem type is typically used as a layer on top of another."""
        return self.category in {"archive", "transform", "wrapper"}

    @property
    def requires_target_fs(self) -> bool:
        """Whether this filesystem type typically requires a target filesystem."""
        return self.category in {"archive", "transform"}

    @classmethod
    def get_available_configs(cls) -> dict[str, type[FileSystemConfig]]:
        """Return all available filesystem configurations.

        Returns:
            Dictionary mapping fs_type values to configuration classes
        """
        result = {}
        for subclass in cls.__subclasses__():
            # Recursively collect subclasses of subclasses too
            result.update(subclass.get_available_configs())

            # Add this subclass if it has a literal fs_type
            if hasattr(subclass, "fs_type") and hasattr(subclass.fs_type, "__args__"):
                fs_type = subclass.fs_type.__args__[0]  # pyright: ignore
                result[fs_type] = subclass

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FileSystemConfig:
        """Create appropriate config instance based on fs_type.

        Args:
            data: Dictionary containing configuration data with fs_type

        Returns:
            Instantiated configuration object of the appropriate type

        Raises:
            ValueError: If fs_type is missing or unknown
        """
        fs_type = data.get("fs_type")
        if not fs_type:
            msg = "fs_type must be specified"
            raise ValueError(msg)

        configs = cls.get_available_configs()
        if fs_type in configs:
            return configs[fs_type](**data)
        return cls(**data)

    def create_fs(self) -> AbstractFileSystem:
        """Create a filesystem instance based on this configuration.

        If layers are specified, creates a layered filesystem using the nested configs.
        Otherwise, creates a simple filesystem using just this configuration.

        Returns:
            Instantiated filesystem with the proper configuration

        Example:
            For a config with layers=[file_config], this is equivalent
            to "zip::file://path/to/file.zip"
        """
        if self.layers:
            # Handle layered filesystem
            all_layers = [self, *self.layers]

            url_kwargs = {}
            for layer in all_layers:
                # Get kwargs excluding special fields
                layer_kwargs = layer.model_dump(
                    exclude={"fs_type", "layers", "target_path", "root_path"}
                )
                layer_kwargs = {k: v for k, v in layer_kwargs.items() if v is not None}

                # For archives, add target_path as 'fo' if not already set
                if (
                    layer.category == "archive"
                    and layer.target_path
                    and "fo" not in layer_kwargs
                ):
                    layer_kwargs["fo"] = layer.target_path

                if layer_kwargs:
                    url_kwargs[layer.fs_type] = layer_kwargs

            # Build protocol string
            base_protocols = []
            for layer in all_layers:
                if layer.root_path:
                    # Insert dir:: modifier with the specified root_path
                    base_protocols.append(f"dir::{layer.fs_type}")
                    # Add root_path to kwargs for this layer
                    if "dir" not in url_kwargs:
                        url_kwargs["dir"] = {}
                    url_kwargs["dir"][layer.fs_type] = {"path": layer.root_path}
                else:
                    base_protocols.append(layer.fs_type)

            protocol_str = "::".join(base_protocols)

            # Get the innermost target_path if it's a base filesystem
            final_path = ""
            innermost_layer = self.layers[-1] if self.layers else None
            if (
                innermost_layer
                and innermost_layer.category == "base"
                and innermost_layer.target_path
            ):
                final_path = f"/{innermost_layer.target_path}"

            url = f"{protocol_str}://{final_path}"

            fs, _ = fsspec.core.url_to_fs(url, **url_kwargs)
            return fs

        # Simple filesystem
        fs_kwargs = self.model_dump(
            exclude={"fs_type", "layers", "target_path", "root_path"}
        )
        fs_kwargs = {k: v for k, v in fs_kwargs.items() if v is not None}

        # Handle target_path based on filesystem category
        if self.target_path and self.category == "archive":
            fs_kwargs["fo"] = self.target_path

        # Apply dir:: modifier if root_path is specified
        if self.root_path:
            # Create the filesystem first
            base_fs = fsspec.filesystem(self.fs_type, **fs_kwargs)
            # Then wrap with dir:: modifier
            return base_fs.chdir(self.root_path)

        return fsspec.filesystem(self.fs_type, **fs_kwargs)

    def create_upath(self, path: str = "/") -> UPath:
        """Create a UPath object for the specified path on this filesystem.

        Args:
            path: Path within the filesystem (defaults to root)

        Returns:
            UPath object for the specified path
        """
        fs = self.create_fs()
        return UPath(path, fs=fs)


if __name__ == "__main__":
    # Example usage
    path = "C:/Users/phili/Downloads/tags.zip"
    file_config = FileSystemConfig(fs_type="file", target_path=path)
    config = FileSystemConfig(fs_type="zip", layers=[file_config])
    fs = config.create_fs()
    print(fs.ls("tags/"))
