import { JsonNode, parse as JSONparse } from 'json-in-order';


export function parseLinesToStructureArray(jsonLinesSource: string): Array<JsonNode> {
    return jsonLinesSource.split(/\r?\n/)
        .map(s => s.trim())
        .filter(x => x.length > 1)
        .map(line => {
            try {
                const out = JSONparse(line)
                return out
            } catch (e) {
                console.warn(e)
                console.log("from line:", line)
                return null
            }
        })
}