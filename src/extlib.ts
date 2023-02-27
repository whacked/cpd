export function last<T>(coll: Array<T>): T {
    return coll[coll.length - 1]
}

export function groupBy<T>(
    coll: Array<T>,
    keySelector: string | ((item: T) => string)
): Record<string, Array<T>> {
    const out: Record<string, Array<T>> = {}
    const keySelectorFunction: (item: T) => string =
        typeof keySelector == "string"
            ? (item: T) => keySelector
            : keySelector
    for (const item of coll) {
        const key = keySelectorFunction(item)
        if (out[key] == null) {
            out[key] = []
        }
        out[key].push(item)
    }
    return out
}