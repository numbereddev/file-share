export const MAX_ENTRIES = 100_000;

export function pathParts(path) {
    if (typeof path !== "string" || path.length > 4096) throw new Error("Invalid relative path.");
    const parts = path.split("/");
    if (
        parts.length > 128 ||
        parts.some(
            (part) => !part || part === "." || part === ".." || /[\\\x00-\x1f\x7f]/.test(part),
        )
    ) {
        throw new Error("Invalid relative path.");
    }
    return parts;
}

function collector() {
    const entries = new Map();
    let bytes = 0;
    let files = 0;

    return {
        add(path, file) {
            pathParts(path);
            if (entries.has(path)) {
                if (!file && !entries.get(path).file) return;
                throw new Error(`Duplicate name: ${path}`);
            }
            if (entries.size >= MAX_ENTRIES)
                throw new Error(
                    `A share can contain at most ${MAX_ENTRIES.toLocaleString()} entries.`,
                );
            entries.set(path, { path, file });
            if (file) {
                files++;
                bytes += file.size;
                if (!Number.isSafeInteger(bytes)) throw new Error("This selection is too large.");
            }
        },
        result() {
            const list = [...entries.values()];
            if (!list.length) throw new Error("No files or folders selected.");
            const roots = new Set(list.map(({ path }) => path.split("/")[0]));
            const label = roots.size === 1 ? [...roots][0].slice(0, 255) : `${roots.size} items`;
            return { entries: list, summary: { label, bytes, files, entries: list.length } };
        },
    };
}

export function fromFiles(files) {
    const output = collector();
    for (const file of files) {
        const path = file.webkitRelativePath || file.name;
        const parts = pathParts(path);
        for (let i = 1; i < parts.length; i++) output.add(parts.slice(0, i).join("/"));
        output.add(path, file);
    }
    return output.result();
}

export async function fromDirectory(handle) {
    const output = collector();
    async function visit(entry, path) {
        if (entry.kind === "file") {
            output.add(path, await entry.getFile());
        } else {
            output.add(path);
            for await (const child of entry.values()) await visit(child, `${path}/${child.name}`);
        }
    }
    await visit(handle, handle.name);
    return output.result();
}

export async function fromDrop(data) {
    // Drop items must be captured synchronously while the drag data store is readable.
    const items = Array.from(data.items || []).filter((item) => item.kind === "file");
    const dropped = items
        .map((item) => item.webkitGetAsEntry?.() || item.getAsFile())
        .filter(Boolean);
    if (!dropped.length) return fromFiles(data.files);

    const output = collector();
    async function visit(entry, path) {
        if (entry.isFile) {
            output.add(path, await new Promise((resolve, reject) => entry.file(resolve, reject)));
        } else if (entry.isDirectory) {
            output.add(path);
            const reader = entry.createReader();
            while (true) {
                const batch = await new Promise((resolve, reject) =>
                    reader.readEntries(resolve, reject),
                );
                if (!batch.length) break;
                for (const child of batch) await visit(child, `${path}/${child.name}`);
            }
        } else {
            output.add(path, entry);
        }
    }
    for (const entry of dropped) await visit(entry, entry.name);
    return output.result();
}
