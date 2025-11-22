"""Tests for E2B filesystem implementation."""

import pytest

from upathtools.filesystems.vercel_fs import VercelFS


@pytest.fixture(scope="session")
async def shared_vercel_fs():
    """Create shared E2B filesystem instance for all tests."""
    fs = VercelFS(template="code-interpreter-v1")
    yield fs
    await fs.close_session()


@pytest.mark.integration
async def test_vercel_session_management():
    """Test session creation and cleanup."""
    fs = VercelFS()
    content = await fs._ls()
    assert isinstance(content, list)


if __name__ == "__main__":
    pytest.main(["-v", __file__, "-m", "integration"])
