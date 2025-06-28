import { JSONSchema7 } from "json-schema"
import { getDataTableName } from "../src/schema-tracking"
import { getOrderedYamlStructure } from "../src/yamlFileApi"


export function extractJsonLinesFromYaml(yamlSource: string): string {
    const orderedSchemaLookup = getOrderedYamlStructure(yamlSource)
    const dataTableName = getDataTableName(orderedSchemaLookup)
    const dataTableMatchPattern = new RegExp(`^${dataTableName}:\s*$`)

    const lines = yamlSource.split(/\r?\n/)
    const jsonLines: Array<string> = []
    let isReading = false
    for (const line of lines) {
        if (isReading) {
            const cleanedLine = line.replace(/^\s*-\s*/, "").trim()
            if (cleanedLine.startsWith('#')) {
                continue
            }
            cleanedLine.length > 0 && jsonLines.push(cleanedLine)
        } else {
            if (line.match(dataTableMatchPattern)) {
                isReading = true
            }
        }
    }
    return jsonLines.join("\n")
}

export function getFirstUsableType(typeCandidates: string | Array<string>) {
    if (Array.isArray(typeCandidates)) {
        for (let i = 0; i < typeCandidates.length; ++i) {
            if (typeCandidates[i] == "null") {
                continue
            } else {
                return typeCandidates[i]
            }
        }
        return "null"
    } else {
        return typeCandidates
    }
}


function getTableNameFromDataTableSchema(dataTableSchema: JSONSchema7) {
    const rowItemSchema = dataTableSchema.items as JSONSchema7
    const tableName = rowItemSchema?.title ?? "data"
    return tableName
}

export function generateTableSqlFromJsonSchema(dataTableSchema: JSONSchema7) {
    if (dataTableSchema.type != "array") {
        throw new Error("Expected data table to be an array")
    }
    const rowItemSchema = dataTableSchema.items as JSONSchema7

    const tableName = getTableNameFromDataTableSchema(dataTableSchema)
    const columns = rowItemSchema?.properties ?? {}
    const columnNames = Object.keys(columns)
    const columnDefinitions = columnNames.map(columnName => {
        const column = columns[columnName] as any  // JSONSchema7Definition not playing nice with .type below
        const dataType = getFirstUsableType(column.type)
        // expected to be a primitive type: string, number, boolean, null
        const columnType = dataType == "string" ? "text" : dataType
        const columnDefinition = `"${columnName}" ${columnType}`
        return columnDefinition
    })
    const columnDefinitionsText = columnDefinitions.join(",\n")
    const sql = `CREATE TABLE ${tableName} (\n${columnDefinitionsText}\n);`
    return sql
}


export function generateSqlForDataTable(dataTableSchema: JSONSchema7, dataTableRows: Array<any>): string {
    const createTableSql = generateTableSqlFromJsonSchema(dataTableSchema);
    const tableName = getTableNameFromDataTableSchema(dataTableSchema)
    const rowItemSchema = (dataTableSchema.items as JSONSchema7).properties
    const columnNames = Object.keys(rowItemSchema ?? {})
    const insertRowsSql = dataTableRows.map((row) => {
        const rowData = columnNames.map((columnName) => {
            const columnValue = row[columnName]
            if (columnValue == null) {
                return "NULL"
            }
            if (getFirstUsableType((rowItemSchema as any)[columnName].type) == "string") {
                return `"${columnValue}"`
            } else {
                return columnValue
            }
        }).join(", ")
        return `INSERT INTO ${tableName} VALUES (${rowData});`
    })
    return [createTableSql, ...insertRowsSql].join("\n")
}