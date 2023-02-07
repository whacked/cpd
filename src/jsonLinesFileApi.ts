import { JsonNode, parse as JSONparse } from 'json-in-order';


export function parseLinesToStructureArray(jsonLines: string): Array<JsonNode> {
    return jsonLines.split(/\r?\n/).map(line => {
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