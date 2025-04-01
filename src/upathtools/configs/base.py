"""Configuration models for filesystem implementations."""

from __future__ import annotations

from typing import Any

import fsspec
from fsspec import AbstractFileSystem
from pydantic import BaseModel, ConfigDict, Field


class FileSystemConfig(BaseModel):
    """Base configuration for filesystem implementations."""

    model_config = ConfigDict(extra="allow", use_attribute_docstrings=True)

    fs_type: str
    """Type of filesystem"""

    path: str | None = None
    """Path for this filesystem layer"""

    layers: list[FileSystemConfig] = Field(default_factory=list)
    """Optional list of nested filesystem configurations in order from outermost to innermost"""  # noqa: E501

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

    def create_fs(self, target_path: str | None = None) -> AbstractFileSystem:
        """Create a filesystem instance based on this configuration.

        If layers are specified, creates a layered filesystem using the nested configs.
        Otherwise, creates a simple filesystem using just this configuration.

        Args:
            target_path: Optional target path to override the path specified in config

        Returns:
            Instantiated filesystem with the proper configuration

        Example:
            For a config with layers=[file_config], this is equivalent
            to "zip::file://path/to/file.zip"
        """
        if self.layers:
            # For layered filesystems, we need to handle path differently
            # The innermost layer (self) should be first in URL, outermost layer last
            # So for FileSystemConfig(fs_type="zip"
            #  layers=[FileSystemConfig(fs_type="file")])
            # We want "zip::file://"
            all_layers = [self, *self.layers]

            url_kwargs = {}
            for layer in all_layers:
                # Extract kwargs excluding fs_type, layers, and path
                layer_kwargs = layer.model_dump(exclude={"fs_type", "layers", "path"})
                layer_kwargs = {k: v for k, v in layer_kwargs.items() if v is not None}

                if layer_kwargs:  # Only add if there are actual kwargs
                    url_kwargs[layer.fs_type] = layer_kwargs

            # Build protocol string - first is self, then layers in order
            protocol_str = "::".join(layer.fs_type for layer in all_layers)

            # Use target_path if provided, otherwise use path from the last layer
            final_path = target_path
            if not final_path and self.layers and self.layers[-1].path:
                final_path = self.layers[-1].path

            if not final_path:
                # No path specified
                url = f"{protocol_str}://"
            else:
                # Use the specified path
                url = f"{protocol_str}://{final_path}"

            # For debugging
            print(f"Creating layered filesystem with URL: {url}")

            fs, _ = fsspec.core.url_to_fs(url, **url_kwargs)
            return fs

        # For simple filesystems, just pass kwargs directly
        fs_kwargs = self.model_dump(exclude={"fs_type", "layers", "path"})
        fs_kwargs = {k: v for k, v in fs_kwargs.items() if v is not None}

        return fsspec.filesystem(self.fs_type, **fs_kwargs)


if __name__ == "__main__":
    # Example usage for zip file
    path = "C:/Users/phili/Downloads/tags.zip"
    file_config = FileSystemConfig(fs_type="file", path=path)
    # Correct order: zip is the outermost layer, file is the innermost layer
    # This should create a filesystem equivalent to "zip::file://C:/Users/phili/Downloads/tags.zip"
    config = FileSystemConfig(fs_type="zip", layers=[file_config])
    fs = config.create_fs()
    print(fs.ls("tags/"))
