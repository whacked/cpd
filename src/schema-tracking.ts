import { SchemaId } from './database-api'
import { JSONSchema7 } from "json-schema"
import Ajv from 'ajv'
import { canonicalize } from "json-canonicalize"
import { _SCHEMA_TABLE_NAME } from "./definitions"
// @ts-ignore
import * as GenerateSchema from 'generate-schema';
import { groupBy, last } from './extlib'


export const ajv = new Ajv({ strict: false })

export type OrderPreservingObject = null | string | number | boolean | Array<OrderPreservingObject> | Map<string, OrderPreservingObject>
export type SchemaMap = Map<string, OrderPreservingObject>


export function toCanonicalKey(serializable: Object) {
    // WARN: consider better key control method
    return JSON.stringify(serializable)
}

export function discoverDelimiter(stringExamples: Array<string>): string {
    const stats: Record<string, number> = {}
    for (const example of stringExamples) {
        for (const candidateDelimiter of [
            ",", "\t", String.fromCharCode(30),
        ].map(x => new RegExp(x, "g"))) {
            const numMatches = Array.from(example.matchAll(candidateDelimiter)).length
            stats[candidateDelimiter.source] = (stats[candidateDelimiter.source] ?? 0) + numMatches
        }
    }
    return Object.entries(stats).map(([key, count]) => {
        return [count, key]
    }).sort((cka, ckb) => {
        return cka[0] > ckb[0] ? -1 : 1
    })[0][1] as string
}

// FIXME: move me
export namespace SchemaTracker {
    const canonicalizedSchemaToId: Record<string, number> = {}
    const schemaIdToSchema: Record<number, any> = {}

    export function getIdToSchemaMapping(): Record<number, any> {
        return {
            ...schemaIdToSchema
        }
    }

    export function getSchemaId(jsObject: any): number {
        const canonicalized = canonicalize(jsObject)
        const existingId = canonicalizedSchemaToId[canonicalized]
        if (existingId != null) {
            return existingId
        } else {
            const newId = Object.keys(canonicalizedSchemaToId).length + 1
            canonicalizedSchemaToId[canonicalized] = newId
            schemaIdToSchema[newId] = { ...jsObject }
            return newId
        }
    }
}

export function getDataTableName(orderedSchemaLookup: Map<string, OrderPreservingObject>): string {
    const topLevelTableNames = Array.from(orderedSchemaLookup.keys())
    const finalTableName = topLevelTableNames[topLevelTableNames.length - 1]
    return finalTableName
}

export function getOrderedSchemaLookupForDataTable(orderedSchemaLookup: Map<string, OrderPreservingObject>): SchemaMap {
    const finalTableName = getDataTableName(orderedSchemaLookup)
    const tableSchemaLookup = (orderedSchemaLookup.get(_SCHEMA_TABLE_NAME) ?? new Map()) as SchemaMap
    // this must be an array
    const finalTableSchema = tableSchemaLookup.get(finalTableName) as SchemaMap
    if (finalTableSchema == null) {
        return new Map()
    } else if (finalTableSchema.get("type") != "array") {
        throw Error("final table schema must define an array")
    } else {
        const itemPropertiesSchemaMap = (finalTableSchema.get("items") as SchemaMap).get("properties") as SchemaMap
        return itemPropertiesSchemaMap
    }
}

export function discoverSchema(jsObject: any) {
    // check if jsObject is a Map
    if (jsObject instanceof Map) {
        jsObject = Object.fromEntries(jsObject.entries())
    }
    const schema = GenerateSchema.json(
        jsObject,
    )
    return schema
}

export class TypeTracker {
    private typeLookup: Record<string, string>
    private typeStatistics: Record<string, Record<string, number>>
    constructor(explicitTypeLookup: Record<string, string> = {}) {
        this.typeLookup = {}
        this.typeStatistics = Object.fromEntries(Object.keys(this.typeLookup).map((key) => {
            return [key, {}]
        }))
        for (const [key, defaultType] of Object.entries(explicitTypeLookup)) {
            this.incrementKeyType(key, defaultType)
        }
    }

    incrementKeyType(key: string, typeName: string) {
        if (this.typeStatistics[key] == null) {
            this.typeStatistics[key] = {}
        }
        this.typeStatistics[key][typeName] = (this.typeStatistics[key][typeName] ?? 0) + 1

        // infer best type based on known examples
        const mostNumerousType = Object.entries(this.typeStatistics[key]).sort(
            (a, b) => a[1] > b[1] ? -1 : 1)[0][0]
        // console.log("setting key", key, "to type", mostNumerousType, this.typeStatistics[key])
        this.typeLookup[key] = mostNumerousType
    }

    getType(key: string): string {
        if (this.typeLookup[key] == null) {
            return "unknown"
        }
        const candidates: Array<[number, string]> = []
        const mostNumerousType = Object.entries(this.typeLookup[key]).map(([typeName, count]) => {
            return [count, typeName]
        }).sort((a, b) => a[0] > b[0] ? -1 : 1)[0][0]
        return mostNumerousType
    }
}

export interface RollingSchemaData {
    schemaLookup: Record<SchemaId, {
        autoSchemaId: number,
        definition: JSONSchema7
    }>
    rowsWithSchema: Array<{
        schemaId: number
        originalData: any
    }>
}

export function rollingSchemaDiscoverer(rows: Array<any>, orderedSchemaLookup?: SchemaMap): RollingSchemaData {
    if (orderedSchemaLookup == null) {
        orderedSchemaLookup = new Map<string, OrderPreservingObject>()
        orderedSchemaLookup.set("data", new Map())
    }
    const out: RollingSchemaData = {
        schemaLookup: {},
        rowsWithSchema: [],
    }

    const dataTableOrderedSchema = getOrderedSchemaLookupForDataTable(orderedSchemaLookup)
    const defaultDefinedFieldTypes = Object.fromEntries(
        Array.from(
            dataTableOrderedSchema.entries()
        ).map(([fieldName, jsonSchemaMap]) => {
            const typeDeclaration = (jsonSchemaMap as SchemaMap).get("type")
            if (Array.isArray(typeDeclaration)) {
                // WARNING brittle
                for (const typeName of typeDeclaration) {
                    if (typeName == "null") {
                        continue
                    }
                    return [fieldName as string, typeName as string]
                }
                return [fieldName as string, null]
            } else {
                return [fieldName as string, typeDeclaration as string]
            }
        }).filter(x => x[1] != null)
    )
    const defaultFieldNames = Array.from(dataTableOrderedSchema.keys())
    const typeTracker = new TypeTracker(defaultDefinedFieldTypes)
    // tableName -> fieldName -> fieldId (auto-increment based on appearance)
    const uniqueValueTrackers: Record<string, Record<string, number>> = {}

    let stringRowIndexes: Array<number> = []
    let stringRowParser: ((rowString: string) => any) | null = null

    for (let i = 0; i < rows.length; ++i) {
        let row = rows[i]
        let originalRowEntry: Record<string, any> | null = null

        if (typeof row == "string") {
            if (stringRowIndexes.length < 2) {
                stringRowIndexes.push(i)
            }
            if (stringRowIndexes.length > 1 && stringRowParser == null) {
                const stringSplitDelimiter = discoverDelimiter(stringRowIndexes.map(
                    rowIndex => rows[rowIndex]
                ))
                stringRowParser = (rowString: string) => {
                    // WARN no nested values or quoted delimiter!
                    const split = rowString.split(stringSplitDelimiter)
                    return Object.fromEntries(split.map((value, index) => {
                        return [defaultFieldNames[index], value]
                    }))
                }
                // backfill
                for (const earlierRowIndex of stringRowIndexes.slice(0, stringRowIndexes.length - 1)) {
                    out.rowsWithSchema[earlierRowIndex].originalData = stringRowParser(
                        out.rowsWithSchema[earlierRowIndex].originalData,
                    )
                }
            }

            if (stringRowParser != null) {
                originalRowEntry = stringRowParser(row)
            }
        } else if (Array.isArray(row)) {
            if (row.length > defaultFieldNames.length) {
                throw Error(`Cannot match: array contains ${row.length} elements by schema only defines ${defaultFieldNames.length} properties`)
            }
            originalRowEntry = Object.fromEntries(row.map((value, index) => {
                return [defaultFieldNames[index], value]
            }))
        } else {
            originalRowEntry = row
        }

        // foreign key substitution
        if (originalRowEntry != null) {
            for (const key of Object.keys(originalRowEntry)) {
                const value = originalRowEntry[key]
                if (value == null) {
                    continue
                }

                let valueTypeMatchesExpectedType: boolean = true
                let expectedTypeDeclaration: string | Array<string> = typeof value

                const explicitPropertySchema = dataTableOrderedSchema.get(key)
                if (explicitPropertySchema) {
                    expectedTypeDeclaration = (explicitPropertySchema as SchemaMap).get("type") as any
                } else {
                    expectedTypeDeclaration = typeTracker.getType(key)
                }
                valueTypeMatchesExpectedType = Array.isArray(expectedTypeDeclaration)
                    ? expectedTypeDeclaration.indexOf(typeof value) > -1
                    : typeof value == expectedTypeDeclaration

                if (valueTypeMatchesExpectedType) {
                    if (uniqueValueTrackers[key] == null) {
                        uniqueValueTrackers[key] = {}
                    }
                    if (uniqueValueTrackers[key][value] == null) {
                        const autoIncrementId = Object.keys(uniqueValueTrackers[key]).length / 2 + 1
                        uniqueValueTrackers[key][value] = autoIncrementId
                        uniqueValueTrackers[key][autoIncrementId] = value
                    }
                } else {
                    const tableRowId = value as number
                    const tableRowIndex = tableRowId - 1
                    const joinableTable = orderedSchemaLookup.get(key)
                    if (joinableTable) {
                        const foreignKeyValue = Array.from(joinableTable as any)[tableRowIndex]
                        originalRowEntry[key] = foreignKeyValue
                    } else if (expectedTypeDeclaration == "string") {
                        // lookup from tracked
                        // console.log(`looking up\t${key} = ${value}\tfrom tracked: ${uniqueValueTrackers[key][value]}`)
                        originalRowEntry[key] = uniqueValueTrackers[key][value]
                    }
                    // console.log("### SUBST", key, expectedTypeDeclaration, "from", value, " ==> ", foreignKeyValue)
                }
            }
        }

        // type tracking
        for (const [key, value] of Object.entries(originalRowEntry ?? {})) {
            typeTracker.incrementKeyType(key, typeof value)
        }

        // console.log(uniqueValueTrackers)

        // console.log(i, typeof row)
        const rowSchema = discoverSchema(row)
        // console.log("SCHEMA", rowSchema)
        const schemaId = SchemaTracker.getSchemaId(rowSchema)
        out.rowsWithSchema.push({
            schemaId,
            originalData: originalRowEntry ?? row,
        })
    }
    return {
        ...out,
        schemaLookup: Object.fromEntries(
            Object.entries(SchemaTracker.getIdToSchemaMapping()).map(
                ([id, schema]) => [id, {
                    autoSchemaId: parseInt(id),
                    definition: schema as JSONSchema7
                }]
            )
        ) as Record<string, {
            autoSchemaId: number,
            definition: JSONSchema7,
        }>,
    }
}

export interface IndexedSchemaRow {
    indexInData: number,
    schema: JSONSchema7,
}

export function deriveSupersetSchema(
    schemas: Array<JSONSchema7>,
    keyAcceptanceEvaluator: (
        key: string,
        matchingSchemas: Array<IndexedSchemaRow>,
    ) => boolean = () => true,
): JSONSchema7 {
    const out: JSONSchema7 = {
        type: "object",
        properties: {},
    }
    const schemaTracker: Record<string, Array<IndexedSchemaRow>> = {}
    for (let i = 0; i < schemas.length; ++i) {
        const schema = schemas[i]
        for (const [key, value] of Object.entries(schema.properties ?? {})) {
            if (schemaTracker[key] == null) {
                schemaTracker[key] = []
            }
            schemaTracker[key].push({
                indexInData: i,
                schema: value as JSONSchema7,
            })
        }
    }
    for (const [key, matchingSchemas] of Object.entries(schemaTracker)) {
        if (keyAcceptanceEvaluator(key, matchingSchemas)) {
            out.properties![key] = last(matchingSchemas).schema
        }
    }

    return out
}

export function deriveSupersetSchemaFromRollingSchemaData(
    rollingSchemaData: RollingSchemaData,
    minimumRequiredSchemaMatchRatio: number = 0.015,
): JSONSchema7 {
    console.log(Object.keys(rollingSchemaData.schemaLookup).length, "SCHEMAS")
    const groupedSchemas = groupBy(rollingSchemaData.rowsWithSchema, (row) => row.schemaId.toString())
    const totalRows = rollingSchemaData.rowsWithSchema.length
    const repeatedlyUsedSchemas: Array<JSONSchema7> = Object.values(rollingSchemaData.schemaLookup).filter((record) => {
        const schemaId = record.autoSchemaId.toString()
        const numRowsMatchingSchema = groupedSchemas[schemaId].length
        return numRowsMatchingSchema / totalRows > minimumRequiredSchemaMatchRatio
    }).map(x => x.definition)
    console.log(repeatedlyUsedSchemas.length, "schemas to consider")
    return deriveSupersetSchema(repeatedlyUsedSchemas)
}