async function requireNew(directory, name, kind) {
    try {
        if (kind === "directory") await directory.getDirectoryHandle(name);
        else await directory.getFileHandle(name);
    } catch (error) {
        if (error.name === "NotFoundError") return;
        throw error;
    }
    throw new Error(`A file or folder named ${name} already exists.`);
}

export async function openDirectoryDestination(parent) {
    const folder = `File share ${crypto.randomUUID()}`;
    const root = await parent.getDirectoryHandle(folder, { create: true });

    async function locate(parts) {
        let directory = root;
        for (const part of parts.slice(0, -1)) directory = await directory.getDirectoryHandle(part);
        return [directory, parts.at(-1)];
    }

    return {
        folder,
        async addDirectory(parts) {
            const [directory, name] = await locate(parts);
            await requireNew(directory, name, "directory");
            await directory.getDirectoryHandle(name, { create: true });
        },
        async openFile(parts) {
            const [directory, name] = await locate(parts);
            await requireNew(directory, name, "file");
            const handle = await directory.getFileHandle(name, { create: true });
            let writer;
            let aborting;
            function abort() {
                return (aborting ??= (async () => {
                    await writer?.abort().catch(() => {});
                    await directory.removeEntry(name).catch(() => {});
                })());
            }
            try {
                writer = await handle.createWritable();
                return { write: (data) => writer.write(data), close: () => writer.close(), abort };
            } catch (error) {
                await abort();
                throw error;
            }
        },
    };
}
