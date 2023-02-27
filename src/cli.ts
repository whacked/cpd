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
import { deriveSupersetSchema, IndexedSchemaRow, OrderPreservingObject, rollingSchemaDiscoverer } from './schema-tracking';
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

export function cliMain(args: IYarguments): Promise<any> {

    let t0 = Date.now()
    const tok = (args.verbosity ?? 0) > 1
        ? (s = '') => { process.stderr.write(` - ${Date.now() - t0}s ${s == null ? '' : ': ' + s}\n`) }
        : () => { }
    tok('initializing...')

    if (!args.inputFile) {
        console.error("No input file specified")
        return Promise.resolve([])
    }

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

    if (args.toSql
        && args.inputFile.endsWith(".yaml") || args.inputFile.endsWith(".yml")
    ) {
        const yamlFileSource = fs.readFileSync(args.inputFile, 'utf8')
        const doc = yaml.load(yamlFileSource) as YayamYamlDatabaseSchema
        process.stdout.write(generateSqlForDataTable(doc._schemas?.['data'] as JSONSchema7, doc.data ?? []))
        return Promise.resolve([])
    }

    return Promise.resolve([])
}

if (require.main === module) {
    cliMain(new ArgParser<IYarguments>(yargOptions).getArgs())
}