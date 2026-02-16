// Deno runtime type declarations (for IDE support when not using Deno's TS)
declare const Deno: {
  stdin: {
    readable: ReadableStream<Uint8Array>;
  };
  exit(code: number): never;
};

declare const globalThis: Record<string, unknown>;

/**
 * Pyodide Server - JSON-RPC bridge over stdin/stdout
 *
 * Protocol:
 * - Requests: {"id": number, "method": string, "params": object}
 * - Responses: {"id": number, "result": object} or {"id": number, "error": object}
 * - Events (streaming): {"id": number, "event": object}
 * - Ready signal: {"ready": true, "version": string}
 *
 * Methods:
 * - execute: Run Python code, return result
 * - stream: Run Python code, stream events
 * - install: Install packages via micropip
 * - shutdown: Graceful shutdown
 *
 * Filesystem methods:
 * - fs_ls: List directory contents
 * - fs_cat: Read file contents (base64 encoded)
 * - fs_write: Write file contents (base64 encoded)
 * - fs_mkdir: Create directory
 * - fs_rm: Remove file
 * - fs_rmdir: Remove directory
 * - fs_stat: Get file/directory info
 * - fs_exists: Check if path exists
 */

// @ts-ignore: npm: specifier is Deno-specific
import { loadPyodide, type PyodideInterface } from "npm:pyodide@0.27.5";

// Types
type Method =
  | "execute"
  | "stream"
  | "shutdown"
  | "install"
  | "fs_ls"
  | "fs_cat"
  | "fs_write"
  | "fs_mkdir"
  | "fs_rm"
  | "fs_rmdir"
  | "fs_stat"
  | "fs_exists";

interface Request {
  id: number;
  method: Method;
  params?: {
    code?: string;
    packages?: string[];
    path?: string;
    content?: string; // base64 encoded for fs_write
    recursive?: boolean;
  };
}

interface ExecuteResult {
  success: boolean;
  result: unknown;
  stdout: string | null;
  stderr: string | null;
  duration: number;
}

interface StreamEvent {
  type: "started" | "output" | "completed" | "error";
  process_id?: string;
  data?: string;
  stream?: "stdout" | "stderr";
  exit_code?: number;
  duration?: number;
  error?: string;
  error_type?: string;
}

interface FileInfo {
  name: string;
  size: number;
  type: "file" | "directory";
  mtime?: number;
}

// Global state
let pyodide: PyodideInterface | null = null;
let processCounter = 0;

// Captured output buffers
let stdoutBuffer: string[] = [];
let stderrBuffer: string[] = [];

// Streaming callback (set during stream execution)
let streamCallback: ((event: StreamEvent) => void) | null = null;

const setupCode = `
import sys
import io
from contextlib import redirect_stdout, redirect_stderr

# Custom stream that can notify on write
class NotifyingStream(io.StringIO):
    def __init__(self, callback_name, stream_type):
        super().__init__()
        self.callback_name = callback_name
        self.stream_type = stream_type

    def write(self, s):
        if s:  # Only notify for non-empty writes
            import js
            callback = getattr(js, self.callback_name, None)
            if callback:
                callback(s, self.stream_type)
        return super().write(s)

def _anyenv_capture_output():
    """Setup output capture for non-streaming execution."""
    return io.StringIO(), io.StringIO()

def _anyenv_streaming_output():
    """Setup output capture for streaming execution."""
    return NotifyingStream('_anyenv_stream_output', 'stdout'), NotifyingStream('_anyenv_stream_output', 'stderr')
`;

/**
 * Initialize Pyodide with micropip
 */
async function initPyodide(): Promise<void> {
  // Suppress pyodide's console output during loading
  const originalLog = console.log;
  const originalWarn = console.warn;
  console.log = () => {};
  console.warn = () => {};

  try {
    pyodide = await loadPyodide({
      stdout: (msg: string) => {
        stdoutBuffer.push(msg);
        if (streamCallback) {
          streamCallback({
            type: "output",
            data: msg + "\n",
            stream: "stdout",
          });
        }
      },
      stderr: (msg: string) => {
        stderrBuffer.push(msg);
        if (streamCallback) {
          streamCallback({
            type: "output",
            data: msg + "\n",
            stream: "stderr",
          });
        }
      },
    });

    await pyodide.loadPackage("micropip", {
      messageCallback: () => {},
      errorCallback: () => {},
    });

    // Run setup code
    await pyodide.runPythonAsync(setupCode);

    // Register streaming callback in JS global scope
    (globalThis as Record<string, unknown>)._anyenv_stream_output = (
      data: string,
      streamType: string
    ) => {
      if (streamCallback) {
        streamCallback({
          type: "output",
          data: data,
          stream: streamType as "stdout" | "stderr",
        });
      }
    };
  } finally {
    console.log = originalLog;
    console.warn = originalWarn;
  }
}

/**
 * Install Python packages via micropip
 */
async function installPackages(packages: string[]): Promise<void> {
  if (!pyodide) throw new Error("Pyodide not initialized");

  const micropip = pyodide.pyimport("micropip");
  for (const pkg of packages) {
    try {
      await micropip.install(pkg);
    } catch (e) {
      // Log but continue - package might not be available
      stderrBuffer.push(`Warning: Failed to install ${pkg}: ${e}`);
    }
  }
}

// ============================================================================
// Filesystem Operations
// ============================================================================

/**
 * List directory contents
 */
async function fsLs(path: string): Promise<FileInfo[]> {
  if (!pyodide) throw new Error("Pyodide not initialized");

  const code = `
import os
import json

def _fs_ls(path):
    entries = []
    for name in os.listdir(path):
        full_path = os.path.join(path, name)
        try:
            stat = os.stat(full_path)
            entries.append({
                "name": full_path,
                "size": stat.st_size,
                "type": "directory" if os.path.isdir(full_path) else "file",
                "mtime": stat.st_mtime
            })
        except OSError:
            pass
    return entries

json.dumps(_fs_ls(${JSON.stringify(path)}))
`;

  const result = await pyodide.runPythonAsync(code);
  return JSON.parse(result);
}

/**
 * Read file contents (returns base64)
 */
async function fsCat(path: string): Promise<string> {
  if (!pyodide) throw new Error("Pyodide not initialized");

  const code = `
import base64

def _fs_cat(path):
    with open(path, 'rb') as f:
        return base64.b64encode(f.read()).decode('ascii')

_fs_cat(${JSON.stringify(path)})
`;

  return await pyodide.runPythonAsync(code);
}

/**
 * Write file contents (expects base64)
 */
async function fsWrite(path: string, contentBase64: string): Promise<void> {
  if (!pyodide) throw new Error("Pyodide not initialized");

  const code = `
import base64
import os

def _fs_write(path, content_b64):
    content = base64.b64decode(content_b64)
    # Ensure parent directory exists
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)
    with open(path, 'wb') as f:
        f.write(content)

_fs_write(${JSON.stringify(path)}, ${JSON.stringify(contentBase64)})
`;

  await pyodide.runPythonAsync(code);
}

/**
 * Create directory
 */
async function fsMkdir(path: string, recursive: boolean = true): Promise<void> {
  if (!pyodide) throw new Error("Pyodide not initialized");

  const recursivePy = recursive ? "True" : "False";
  const code = `
import os

def _fs_mkdir(path, recursive):
    if recursive:
        os.makedirs(path, exist_ok=True)
    else:
        os.mkdir(path)

_fs_mkdir(${JSON.stringify(path)}, ${recursivePy})
`;

  await pyodide.runPythonAsync(code);
}

/**
 * Remove file
 */
async function fsRm(path: string): Promise<void> {
  if (!pyodide) throw new Error("Pyodide not initialized");

  const code = `
import os

def _fs_rm(path):
    os.remove(path)

_fs_rm(${JSON.stringify(path)})
`;

  await pyodide.runPythonAsync(code);
}

/**
 * Remove directory
 */
async function fsRmdir(path: string, recursive: boolean = false): Promise<void> {
  if (!pyodide) throw new Error("Pyodide not initialized");

  const recursivePy = recursive ? "True" : "False";
  const code = `
import os
import shutil

def _fs_rmdir(path, recursive):
    if recursive:
        shutil.rmtree(path)
    else:
        os.rmdir(path)

_fs_rmdir(${JSON.stringify(path)}, ${recursivePy})
`;

  await pyodide.runPythonAsync(code);
}

/**
 * Get file/directory info
 */
async function fsStat(path: string): Promise<FileInfo> {
  if (!pyodide) throw new Error("Pyodide not initialized");

  const code = `
import os
import json

def _fs_stat(path):
    stat = os.stat(path)
    return json.dumps({
        "name": path,
        "size": stat.st_size,
        "type": "directory" if os.path.isdir(path) else "file",
        "mtime": stat.st_mtime
    })

_fs_stat(${JSON.stringify(path)})
`;

  const result = await pyodide.runPythonAsync(code);
  return JSON.parse(result);
}

/**
 * Check if path exists
 */
async function fsExists(path: string): Promise<boolean> {
  if (!pyodide) throw new Error("Pyodide not initialized");

  const code = `
import os

def _fs_exists(path):
    return os.path.exists(path)

_fs_exists(${JSON.stringify(path)})
`;
  return await pyodide.runPythonAsync(code);
}

/**
 * Auto-detect and install imports from code
 */
async function autoInstallImports(code: string): Promise<void> {
  if (!pyodide) throw new Error("Pyodide not initialized");

  // Use Pyodide's find_imports
  const findImportsCode = `
import sys
try:
    from pyodide.code import find_imports
except ImportError:
    from pyodide import find_imports

def _find_missing_imports(code):
    imports = find_imports(code)
    missing = []
    for module in imports:
        try:
            __import__(module.split('.')[0])
        except ImportError:
            missing.append(module.split('.')[0])
    return missing

_find_missing_imports(${JSON.stringify(code)})
`;

  try {
    const missing = await pyodide.runPythonAsync(findImportsCode);
    const missingList = missing.toJs() as string[];
    if (missingList.length > 0) {
      await installPackages(missingList);
    }
  } catch {
    // Syntax error in code - will be caught during execution
  }
}

/**
 * Execute Python code (non-streaming)
 */
async function execute(code: string): Promise<ExecuteResult> {
  if (!pyodide) throw new Error("Pyodide not initialized");

  const startTime = performance.now();
  stdoutBuffer = [];
  stderrBuffer = [];

  try {
    // Auto-install missing imports
    await autoInstallImports(code);

    // Execute the code
    const result = await pyodide.runPythonAsync(code);

    // Convert result to JS
    let jsResult: unknown = null;
    if (result !== undefined && result !== null) {
      try {
        if (typeof result.toJs === "function") {
          jsResult = result.toJs({ dict_converter: Object.fromEntries });
        } else {
          jsResult = result;
        }
      } catch {
        jsResult = String(result);
      }
    }

    return {
      success: true,
      result: jsResult,
      stdout: stdoutBuffer.length > 0 ? stdoutBuffer.join("\n") : null,
      stderr: stderrBuffer.length > 0 ? stderrBuffer.join("\n") : null,
      duration: (performance.now() - startTime) / 1000,
    };
  } catch (error: unknown) {
    const errorMessage =
      error instanceof Error ? error.message : String(error);
    return {
      success: false,
      result: null,
      stdout: stdoutBuffer.length > 0 ? stdoutBuffer.join("\n") : null,
      stderr: errorMessage,
      duration: (performance.now() - startTime) / 1000,
    };
  }
}

/**
 * Execute Python code with streaming events
 */
async function* stream(
  code: string,
  _requestId: number
): AsyncGenerator<StreamEvent> {
  if (!pyodide) throw new Error("Pyodide not initialized");

  const processId = `pyodide_${++processCounter}`;
  const startTime = performance.now();

  // Emit started event
  yield {
    type: "started",
    process_id: processId,
  };

  // Setup streaming callback
  const events: StreamEvent[] = [];
  streamCallback = (event: StreamEvent) => {
    events.push({ ...event, process_id: processId });
  };

  try {
    // Auto-install missing imports
    await autoInstallImports(code);

    // Execute the code
    await pyodide.runPythonAsync(code);

    // Yield any buffered events
    for (const event of events) {
      yield event;
    }

    // Emit completed event
    yield {
      type: "completed",
      process_id: processId,
      exit_code: 0,
      duration: (performance.now() - startTime) / 1000,
    };
  } catch (error: unknown) {
    // Yield any buffered events first
    for (const event of events) {
      yield event;
    }

    const errorMessage =
      error instanceof Error ? error.message : String(error);
    const errorType = error instanceof Error ? error.name : "Error";

    yield {
      type: "error",
      process_id: processId,
      error: errorMessage,
      error_type: errorType,
      exit_code: 1,
      duration: (performance.now() - startTime) / 1000,
    };
  } finally {
    streamCallback = null;
  }
}

/**
 * Send JSON response to stdout
 */
function sendResponse(id: number, result: unknown): void {
  console.log(JSON.stringify({ id, result }));
}

/**
 * Send JSON error to stdout
 */
function sendError(id: number, error: string, errorType: string = "Error"): void {
  console.log(JSON.stringify({ id, error: { message: error, type: errorType } }));
}

/**
 * Send streaming event to stdout
 */
function sendEvent(id: number, event: StreamEvent): void {
  console.log(JSON.stringify({ id, event }));
}

/**
 * Process a single request
 */
async function processRequest(request: Request): Promise<boolean> {
  const { id, method, params } = request;

  switch (method) {
    case "execute": {
      if (!params?.code) {
        sendError(id, "Missing 'code' parameter", "InvalidParams");
        return true;
      }
      const result = await execute(params.code);
      sendResponse(id, result);
      return true;
    }

    case "stream": {
      if (!params?.code) {
        sendError(id, "Missing 'code' parameter", "InvalidParams");
        return true;
      }
      for await (const event of stream(params.code, id)) {
        sendEvent(id, event);
      }
      return true;
    }

    case "install": {
      if (!params?.packages || !Array.isArray(params.packages)) {
        sendError(id, "Missing 'packages' parameter", "InvalidParams");
        return true;
      }
      try {
        await installPackages(params.packages);
        sendResponse(id, { success: true });
      } catch (error: unknown) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        sendError(id, errorMessage, "InstallError");
      }
      return true;
    }

    case "shutdown": {
      sendResponse(id, { success: true });
      return false; // Signal to stop the server
    }

    // Filesystem operations
    case "fs_ls": {
      if (!params?.path) {
        sendError(id, "Missing 'path' parameter", "InvalidParams");
        return true;
      }
      try {
        const entries = await fsLs(params.path);
        sendResponse(id, entries);
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        sendError(id, errorMessage, "FileSystemError");
      }
      return true;
    }

    case "fs_cat": {
      if (!params?.path) {
        sendError(id, "Missing 'path' parameter", "InvalidParams");
        return true;
      }
      try {
        const content = await fsCat(params.path);
        sendResponse(id, { content });
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        sendError(id, errorMessage, "FileSystemError");
      }
      return true;
    }

    case "fs_write": {
      if (!params?.path || params.content === undefined) {
        sendError(id, "Missing 'path' or 'content' parameter", "InvalidParams");
        return true;
      }
      try {
        await fsWrite(params.path, params.content);
        sendResponse(id, { success: true });
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        sendError(id, errorMessage, "FileSystemError");
      }
      return true;
    }

    case "fs_mkdir": {
      if (!params?.path) {
        sendError(id, "Missing 'path' parameter", "InvalidParams");
        return true;
      }
      try {
        await fsMkdir(params.path, params.recursive ?? true);
        sendResponse(id, { success: true });
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        sendError(id, errorMessage, "FileSystemError");
      }
      return true;
    }

    case "fs_rm": {
      if (!params?.path) {
        sendError(id, "Missing 'path' parameter", "InvalidParams");
        return true;
      }
      try {
        await fsRm(params.path);
        sendResponse(id, { success: true });
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        sendError(id, errorMessage, "FileSystemError");
      }
      return true;
    }

    case "fs_rmdir": {
      if (!params?.path) {
        sendError(id, "Missing 'path' parameter", "InvalidParams");
        return true;
      }
      try {
        await fsRmdir(params.path, params.recursive ?? false);
        sendResponse(id, { success: true });
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        sendError(id, errorMessage, "FileSystemError");
      }
      return true;
    }

    case "fs_stat": {
      if (!params?.path) {
        sendError(id, "Missing 'path' parameter", "InvalidParams");
        return true;
      }
      try {
        const info = await fsStat(params.path);
        sendResponse(id, info);
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        sendError(id, errorMessage, "FileSystemError");
      }
      return true;
    }

    case "fs_exists": {
      if (!params?.path) {
        sendError(id, "Missing 'path' parameter", "InvalidParams");
        return true;
      }
      try {
        const exists = await fsExists(params.path);
        sendResponse(id, exists);
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        sendError(id, errorMessage, "FileSystemError");
      }
      return true;
    }

    default: {
      sendError(id, `Unknown method: ${method}`, "MethodNotFound");
      return true;
    }
  }
}

/**
 * Read lines from stdin
 */
async function* readLines(): AsyncGenerator<string> {
  const decoder = new TextDecoder();
  let buffer = "";

  for await (const chunk of Deno.stdin.readable) {
    buffer += decoder.decode(chunk, { stream: true });

    const lines = buffer.split("\n");
    buffer = lines.pop() || "";

    for (const line of lines) {
      if (line.trim()) {
        yield line;
      }
    }
  }

  // Handle remaining buffer
  if (buffer.trim()) {
    yield buffer;
  }
}

/**
 * Main server loop
 */
async function main(): Promise<void> {
  // Initialize Pyodide
  await initPyodide();

  // Signal ready
  console.log(JSON.stringify({ ready: true, version: "0.1.0" }));

  // Process requests
  for await (const line of readLines()) {
    try {
      const request = JSON.parse(line) as Request;
      const continueRunning = await processRequest(request);
      if (!continueRunning) {
        break;
      }
    } catch (error: unknown) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      // Parse error - send with id -1
      sendError(-1, `Failed to parse request: ${errorMessage}`, "ParseError");
    }
  }
}

// Run if main module
// @ts-ignore: import.meta.main is Deno-specific
if (import.meta.main) {
  main().catch((err) => {
    console.error(JSON.stringify({ fatal: true, error: String(err) }));
    Deno.exit(1);
  });
}

export {
  execute,
  stream,
  installPackages,
  fsLs,
  fsCat,
  fsWrite,
  fsMkdir,
  fsRm,
  fsRmdir,
  fsStat,
  fsExists,
};
