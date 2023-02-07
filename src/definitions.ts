import { YayamYamlDatabaseSchema } from "./autogen/interfaces/YamlFile"

export interface ValidationResult {
    isValid: boolean
    errors: Array<any>
}


// FIXME move this to autogen
export type ReservedDatabaseTableNames = keyof YayamYamlDatabaseSchema
export const _SCHEMA_TABLE_NAME = '_schemas'
