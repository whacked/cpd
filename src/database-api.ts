import { YayamYamlDatabaseSchema } from "./autogen/interfaces/YamlFile"
import { ajv } from "./schema-tracking"

export function getDataTableNames(yDatabase: YayamYamlDatabaseSchema) {
    return Object.keys(yDatabase).filter((tableName) => {
        return !tableName.startsWith('_')
    })
}

export interface ErrorWithRowIndex {
    rowIndex: number
    errorObject: any
}

export interface TableDataValidationResult {
    isValid: boolean,
    errors: Record<string, Array<ErrorWithRowIndex>>
}

export function validateTableData(yamlObject: YayamYamlDatabaseSchema, verbosity: number = 1): TableDataValidationResult {
    const result: TableDataValidationResult = {
        isValid: true,
        errors: {},
    }
    if (yamlObject._schemas) {
        for (const tableName of getDataTableNames(yamlObject)) {
            const entrySchema = yamlObject._schemas[tableName]
            if (entrySchema == null) {
                if (verbosity > 0) {
                    console.warn(`WARN: table "${tableName}" has no usable schema`)
                }
                continue
            }
            const tableEntryValidator = ajv.compile(entrySchema)

            const rows = yamlObject[tableName]
            if (rows == null) {
                if (verbosity > 0) {
                    console.warn(`WARN: table "${tableName}" has no data`)
                }
                continue
            } else {
                for (let i = 0; i < rows.length; ++i) {
                    const row = rows[i]
                    tableEntryValidator(row)
                    if (tableEntryValidator.errors) {
                        if (result.errors[tableName] == null) {
                            result.errors[tableName] = []
                            result.isValid = false
                        }
                        tableEntryValidator.errors.forEach((error) => {
                            console.warn(`ENTRY VALIDATION FAILURE at row ${i}:`, error)
                            console.log("given>", row)
                            result.errors[tableName].push({
                                rowIndex: i,
                                errorObject: error,
                            })
                        })
                    }
                }
            }
        }
    }
    return result
}

export interface TableStatistics {
    numberOfRows: number,
    maxRecordSize: number,
}

export function getDatabaseStatistics(validatedDatabase: YayamYamlDatabaseSchema) {

    const tableNames = getDataTableNames(validatedDatabase)

    const out: {
        numberOfTables: number,
        tableStatistics: Record<string, TableStatistics>,
    } = {
        numberOfTables: tableNames.length,
        tableStatistics: {}
    }

    for (const tableName of tableNames) {
        let maxRecordSize = 0
        const rows: Array<any> = validatedDatabase[tableName] ?? []
        for (const row of rows) {
            let recordSize = typeof row == "string"
                ? 1
                : Object.keys(row).length
            if (recordSize > maxRecordSize) {
                maxRecordSize = recordSize
            }
        }
        out.tableStatistics[tableName] = {
            numberOfRows: rows.length,
            maxRecordSize,
        }
    }

    return out
}

export type SchemaId = number