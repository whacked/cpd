import Ajv from 'ajv'
import * as fs from 'fs'
import { parse as JSONparse } from 'json-in-order'
import * as path from 'path'
import { Kind, safeLoad, YAMLNode } from 'yaml-ast-parser'
import { YayamYamlDatabaseSchema } from './autogen/interfaces/YamlFile'
import { ValidationResult } from './definitions'
import { OrderPreservingObject } from './schema-tracking'


const ajv = new Ajv({ strict: false })
const yamlFileSchemaValidator = ajv.compile(
    JSONparse(
        fs.readFileSync(
            path.join(
                __dirname, 'autogen/schemas/YamlFile.tagged.schema.json',
            ),
            'utf-8'
        )
    ) as any
)


export function isValidYamlFileContent(yamlObject: YayamYamlDatabaseSchema): ValidationResult {
    const result: ValidationResult = {
        isValid: true,
        errors: [],
    }
    try {
        yamlFileSchemaValidator(yamlObject)
        if (yamlFileSchemaValidator.errors) {
            result.errors = yamlFileSchemaValidator.errors
        } else {
            result.isValid = true
        }
    } catch (e) {
        console.error(e);
    }
    return result
}

export interface RollingSchemaData {
    schemaLookup: Record<RowIndex, {
        autoId: number,
        definition: JSONSchema7
    }>
    rowsWithSchema: Array<{
        schemaId: number
        originalData: any
    }>
}

export function yamlAstToOrderedObjects(yamlAst: YAMLNode): OrderPreservingObject {
    if (yamlAst == null) {
        return null
    }

    switch (yamlAst.kind) {
        case Kind.SEQ:
            return ((yamlAst as any).items as Array<YAMLNode>).map((item) => {
                return yamlAstToOrderedObjects(item)
            })

        case Kind.MAP:
        case Kind.MAPPING:
            const outMap = new Map<string, any>()
            yamlAst.mappings.forEach((mappingItem: any) => {
                const objectKey = mappingItem.key.value
                const objectValue = yamlAstToOrderedObjects(mappingItem.value)
                outMap.set(objectKey, objectValue)
            })
            return outMap

        case Kind.SCALAR:
            return yamlAst.value
    }

    console.error(`FAIL ON KIND ${yamlAst.kind}`, yamlAst)
    throw Error('unhandled AST node')
}

export function getOrderedYamlStructure(yamlSource: string): Map<string, OrderPreservingObject> {
    // return an array of json schemas where the index of the schema
    // corresponds to the order at which the schema appears in the yaml definition
    // such that, when given a field represented as an array:
    // [valueA, valueB, ...]
    // valueA will have schema at i = 0
    // valueB will have schema at i = 1
    const loaded = safeLoad(yamlSource)
    const orderedSchemaLookup = new Map<string, OrderPreservingObject>()
    for (let i = 0; i < loaded.mappings.length; ++i) {
        const topLevelNode = loaded.mappings[i]
        const topLevelEntryKey = topLevelNode.key.value as string
        orderedSchemaLookup.set(topLevelEntryKey, yamlAstToOrderedObjects(topLevelNode.value))
    }
    return orderedSchemaLookup
}
