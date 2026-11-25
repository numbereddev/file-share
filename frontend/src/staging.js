const STAGING_DIR = "one-off-file-share-v1";
const RETAIN_MS = 24 * 60 * 60 * 1000;
const lockName = (name) => `${STAGING_DIR}/${name}`;

function holdLock(locks, name) {
    if (!locks) return Promise.resolve(() => {});
    return new Promise((resolve, reject) => {
        locks
            .request(lockName(name), () => new Promise((release) => resolve(release)))
            .catch(reject);
    });
}

async function prune(root, locks) {
    if (!locks) return;
    const cutoff = Date.now() - RETAIN_MS;
    for await (const name of root.keys()) {
        const match = /^(\d+)-[0-9a-f-]{36}$/.exec(name);
        if (!match || Number(match[1]) >= cutoff) continue;
        await locks.request(lockName(name), { ifAvailable: true }, async (lock) => {
            if (!lock) return;
            try {
                const file = await (await root.getFileHandle(name)).getFile();
                if (file.lastModified < cutoff) await root.removeEntry(name);
            } catch {
                /* The entry may already have been deleted by its owner. */
            }
        });
    }
}

export async function pruneStagedDownloads(storage = navigator.storage, locks = navigator.locks) {
    if (!locks) return;
    const root = await (await storage.getDirectory()).getDirectoryHandle(STAGING_DIR);
    await prune(root, locks);
}

function downloadName(name) {
    return name.replace(/[<>:"/\\|?*\x00-\x1f\x7f]/g, "_").replace(/[. ]+$/, "") || "File share";
}

function archivePath(parts) {
    if (
        parts.some((part) =>
            /[<>:"|?*]|[. ]$|^(?:con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])(?:\.|$)/i.test(part),
        )
    ) {
        throw new Error(
            "This share contains a name that cannot safely be exported in a ZIP archive.",
        );
    }
    return parts.join("/");
}

function zipEntry(zip, path) {
    let controller;
    const stream = new TransformStream({
        start(value) {
            controller = value;
        },
    });
    const input = stream.writable.getWriter();
    const result = zip.add(path, stream.readable);
    // Wake a pending write if zip.js stops reading because the disk write failed.
    void result.catch((error) => controller.error(error));
    return {
        write: (data) => input.write(new Uint8Array(data)),
        async close() {
            await input.close();
            await result;
        },
        async abort() {
            await input.abort().catch(() => {});
            await result.catch(() => {});
        },
    };
}

export async function openStagedDestination(
    summary,
    storage = navigator.storage,
    locks = navigator.locks,
) {
    const root = await (
        await storage.getDirectory()
    ).getDirectoryHandle(STAGING_DIR, { create: true });
    await prune(root, locks).catch(() => {});
    const estimate = await storage.estimate?.().catch(() => null);
    if (
        estimate &&
        Number.isFinite(estimate.quota) &&
        Number.isFinite(estimate.usage) &&
        summary.bytes > estimate.quota - estimate.usage
    ) {
        throw new Error(
            "Not enough browser storage for this share. Try a browser with a save-to-folder picker.",
        );
    }

    const archive = summary.files !== 1 || summary.entries !== 1;
    const ZipWriter = archive ? (await import("@zip.js/zip.js")).ZipWriter : null;
    const name = `${Date.now()}-${crypto.randomUUID()}`;
    const unlock = await holdLock(locks, name);
    let disk, zip, staged, removal, url;
    let filename = `${downloadName(summary.label)}.zip`;
    function discard() {
        return (removal ??= (async () => {
            try {
                if (disk) await disk.abort().catch(() => {});
                await root.removeEntry(name);
            } catch (error) {
                if (error.name !== "NotFoundError") {
                    removal = undefined;
                    throw error;
                }
            }
            if (url) URL.revokeObjectURL(url);
            unlock();
        })());
    }

    try {
        staged = await root.getFileHandle(name, { create: true });
        disk = await staged.createWritable();
        if (archive) {
            // Keep the disk stream unlocked so cancellation can abort it independently of zip.js.
            zip = new ZipWriter(new WritableStream({ write: (bytes) => disk.write(bytes) }), {
                level: 0,
                zip64: true,
                bufferedWrite: false,
                useWebWorkers: false,
            });
        }
    } catch (error) {
        await discard().catch(unlock);
        throw error;
    }

    return {
        async addDirectory(parts) {
            if (!archive) throw new Error("Unexpected folder in single-file share.");
            await zip.add(`${archivePath(parts)}/`, undefined, { directory: true });
        },
        async openFile(parts) {
            if (zip) return zipEntry(zip, archivePath(parts));
            filename = downloadName(parts.at(-1));
            return {
                write: (data) => disk.write(data),
                close: () => {},
                abort: () => disk.abort(),
            };
        },
        async finish() {
            if (zip) await zip.close();
            await disk.close();
            url = URL.createObjectURL(await staged.getFile());
            return { url, filename };
        },
        discard,
        release: unlock,
    };
}
