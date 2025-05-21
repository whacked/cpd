import * as path from 'path'
import { JSONSchema7 } from "json-schema"
import yargs from 'yargs/yargs';
import { Argv } from 'yargs';
import { hideBin } from 'yargs/helpers';
import { YayamYamlDatabaseSchema } from './autogen/interfaces/YamlFile'
import * as yaml from 'js-yaml'
import * as fs from 'fs'
import Ajv from 'ajv'
import { isValidYamlFileContent } from './yamlFileApi';
import { getDatabaseStatistics } from './database-api';
import { generateSqlForDataTable } from '../tests/util';
import { deriveSupersetSchema, deriveSupersetSchemaFromRollingSchemaData, IndexedSchemaRow, OrderPreservingObject, rollingSchemaDiscoverer } from './schema-tracking';
import { parseLinesToStructureArray } from './jsonLinesFileApi';
import { groupBy } from './extlib';


export interface IYarguments {
    inputFile: string,
    toSql?: boolean,
    verbosity?: number,
}

const yargOptions: { [key in keyof IYarguments]: any } = {
    inputFile: {
        alias: 'i',
        type: 'string',
        description: 'path to input file, or "-" for STDIN, to validate',
        nargs: 1,
    },
    toSql: {
        alias: 'q',
        type: 'boolean',
        description: 'output corresponding SQL to use with e.g. sqlite',
    },
    verbosity: {
        alias: 'v',
        type: 'number',
        default: 1,
    },
}

export class ArgParser<T> {

    readonly argParser: Argv;
    constructor(yargOptions: IYarguments) {
        this.argParser = yargs(hideBin(process.argv)).options(yargOptions as any)
    }

    showHelp() {
        return this.argParser.showHelp()
    }

    getArgs(): T {
        let args: any = this.argParser.parseSync()
        return args as T
    }
}

// moveme
export function isYamlFile(fileName: string): boolean {
    return fileName.endsWith(".yaml") || fileName.endsWith(".yml")
}

export function isJsonLinesFile(fileName: string): boolean {
    return fileName.endsWith(".jsonl")
}

export enum FileType {
    YAML = 'yaml',
    JsonLines = 'jsonl',
}

export interface FileStatistics {
    numberOfLines: number,
    modifiedAt: Date,
    createdAt: Date,
    fileSize: number,
    fileName: string,
    fileExtension: string,
    fileType?: FileType,

}

export function getFileStatistics(filePath: string): FileStatistics {
    const stats = fs.statSync(filePath)
    const fileName = path.basename(filePath)
    const fileExtension = path.extname(filePath)
    const sourceLines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/g)

    let fileType: FileType =
        isYamlFile(fileName) ? FileType.YAML :
        isJsonLinesFile(fileName) ? FileType.JsonLines :
        undefined;

    return {
        fileName,
        fileExtension,
        numberOfLines: sourceLines.length,
        modifiedAt: stats.mtime,
        createdAt: stats.ctime,
        fileSize: stats.size,
        fileType: fileType,
    }
}

export function cliMain(inputArguments?: IYarguments): Promise<any> {

    const argParser = new ArgParser<IYarguments>({
        ...yargOptions,
        ...inputArguments,
    })
    const args: IYarguments = argParser.getArgs()

    let t0 = Date.now()
    const tok = (args.verbosity ?? 0) > 1
        ? (s = '') => { process.stderr.write(` - ${Date.now() - t0}s ${s == null ? '' : ': ' + s}\n`) }
        : () => { }
    tok('initializing...')

    console.log(Object.keys(args))
    console.log(Object.values(args))

    if (!args.inputFile
        || false
    ) {
        // show the yargs help to stderr
        argParser.showHelp()
    }

    if (!args.inputFile) {
        console.error("No input file specified")
        return Promise.resolve([])
    }

    if (isYamlFile(args.inputFile)) {
        const doc = yaml.load(fs.readFileSync(args.inputFile, 'utf8')) as YayamYamlDatabaseSchema
        const validationResult = isValidYamlFileContent(doc)
        if (validationResult.isValid) {
            const summary = getDatabaseStatistics(doc)
        } else {
            console.warn("ERRORS FOUND:")
            validationResult.errors.forEach((error) => {
                console.log(error)
            })
        }
    }

    if (isJsonLinesFile(args.inputFile)) {
        const jsonLinesFileSource = fs.readFileSync(args.inputFile, 'utf8')
        console.log("got a json lines file")
        console.log(
            getFileStatistics(args.inputFile)
        )
        const jsonLineRows = parseLinesToStructureArray(jsonLinesFileSource)
        const rollingSchemaData = rollingSchemaDiscoverer(jsonLineRows)
        const supersetSchema = deriveSupersetSchemaFromRollingSchemaData(rollingSchemaData)
        console.log("SUPERSET", supersetSchema)

        // re-validate each entry against the superset schema
        const validator = new Ajv({ strict: false }).compile(supersetSchema)
        let numFailures = 0
        for (let i = 0; i < jsonLineRows.length; i++) {
            const row = jsonLineRows[i]
            validator(row)
            if (validator.errors) {
                numFailures++
                console.log(`failed to validate row ${i}`, row, validator.errors)
            }
        }
        console.log(`VALIDATED ${jsonLineRows.length} rows; ${numFailures} failures`)
        if (numFailures == 0) {
            // print the stringified schema
            console.log(JSON.stringify(supersetSchema, null, 2))
        }
    }

    if (args.toSql && isYamlFile(args.inputFile)) {
        const yamlFileSource = fs.readFileSync(args.inputFile, 'utf8')
        const doc = yaml.load(yamlFileSource) as YayamYamlDatabaseSchema
        process.stdout.write(generateSqlForDataTable(doc._schemas?.['data'] as JSONSchema7, doc.data ?? []))
        return Promise.resolve([])
    }

    return Promise.resolve([])
}

if (require.main === module) {
    cliMain()
}
