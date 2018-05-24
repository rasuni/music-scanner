import * as assertorig from 'assert';
import * as Fiber from 'fibers';
const level = require('level');
const levelgraph = require('levelgraph');
import * as path from 'path';
import * as uuid from 'uuid/v4';
import * as commander from 'commander';
import * as immutable from 'immutable';
import * as fs from 'fs';

commander.version('0.0.1').description('music scanner command line tool');


/*
import * as mm from 'music-metadata';
import * as https from 'https';
import * as sax from 'sax';

interface Directory {
    entries?: {
        [name: string]: number;
    }
}

interface FileSystemEntry extends Directory {
    readonly type: "fileSystemEntry";
    readonly name: string;
    readonly directory: number;
    isDirectory?: boolean;

    duration?: number;
    trackNumber?: number;
    bitsPerSample?: number;
    title?: string;
    totalTracks?: number;
    barcode?: number;
    label?: string;
    albumArtist?: number;
    sampleRate?: number;
    releaseCountry?: string;
    // when directory
    // entry/<name>

    // TODO:
    // when file
    // acoustid?: number    

}

interface Root extends Directory {
    readonly type: "root";
    nextId: number;
    currentTask: number;
    // entry/<name>
}

interface Recording {
    readonly type: "recording";
    readonly mbid: string;
    title?: string;
    //length?: number;
    //artist?: number;
    //acoustId?: number;

    // TODO:

    //release?: number;
    //file/<id>
}

interface Artist {
    readonly type: "artist";
    readonly mbid: string;
    albumArtistFiles?: number[];
    isniList?: number[];
    //typeId?: number;
    //name?: string;
    //sortName?: string;
    //recording?: number;
}

interface AcoustId {
    readonly type: "acoustid";
    readonly acoustid: string;
    score?: number;
    //recording?: number;

    // file/<fileId>
}

interface ArtistType {
    readonly type: "artist-type";
    readonly typeId: string;
    readonly name: string;

    // artist/<id>
}

interface Release {
    readonly type: "release";
    readonly mbid: string;

    // recording/<id>
}

interface Isni {
    readonly type: 'isni';
    readonly isni: string;
    readonly artist: number;
}

type DynamicTask = FileSystemEntry | Recording | Artist | AcoustId | ArtistType | Release | Isni;
type Task = Root | DynamicTask;
type LinkedTask = Linked & Task;


interface LoadedRecord {
    readonly type: "loaded";
    currentContent: any;
    readonly loadedContent?: string;
};

interface DeletedRecord {
    readonly type: "deleted";
};

const DELETED: DeletedRecord = {
    type: "deleted"
}

type Record = LoadedRecord | DeletedRecord;

*/


type Consumer<T> = (result: T) => void;

function waitFor<R>(asyncFunction: (consumer: Consumer<R>) => void): R {
    const fiber = Fiber.current;
    asyncFunction((result: R) => fiber.run(result));
    return Fiber.yield();
}


interface Pair<F, S> {
    readonly first: F,
    readonly second: S
}

type Consumer2<F, S> = (first: F, second: S) => void;

function waitFor2<F, S>(asyncFunction: (consumer: Consumer2<F, S>) => void): Pair<F, S> {
    return waitFor((consumer: (result: Pair<F, S>) => void) => asyncFunction((first: F, second: S) => consumer({
        first: first,
        second: second
    })));
}


function fail(): never {
    debugger;
    //assert.fail("failure");
    assertorig.fail("failure");
    throw new Error('AssertionFailure');
}

function failIf(condition: boolean): void {
    if (condition) {
        fail();
    }
}

function isObject(value: any): boolean {
    return typeof value === 'object';
}

function deepEquals(actual: any, expected: any): boolean {
    if (actual === expected) {
        return true;
    }
    else {
        if (isObject(actual) && isObject(expected)) {
            const actualKeys = Object.keys(actual);
            if (actualKeys.length !== Object.keys(expected).length) {
                return false;
            }
            else {
                for (const key of actualKeys) {
                    if (!deepEquals(actual[key], expected[key])) {
                        return false;
                    }
                }
                return true;
            }
        }
        else {
            return false;
        }
    }
}


function assert(condition: boolean): void {
    failIf(!condition);
}

function assertEquals(actual: any, expected: any): void {
    assert(deepEquals(actual, expected));
}


function isUndefined(value: any): value is undefined {
    return value === undefined;
}

function assertDefined(value: any): void {
    failIf(isUndefined(value));
}

/*

function isUndefined(value: any): value is undefined {
    return value === undefined;
}

function assertDefined(value: any): void {
    failIf(isUndefined(value));
}



interface AddInfo {
    added: boolean,
    id: number
}

interface OpenTagEvent {
    readonly type: 'openTag';
    readonly tag: sax.Tag;
}

interface TextEvent {
    readonly type: 'text';
    readonly text: string;
}

interface CloseTagEvent {
    readonly type: 'closeTag';
    readonly name: string;
}

type SaxEvent = OpenTagEvent | TextEvent | CloseTagEvent;


function fetchNextEvent(nextEvent: () => SaxEvent, type: string): SaxEvent {
    const event = nextEvent();
    assertEquals(event.type, type);
    return event;
}

interface Attributes {
    readonly [name: string]: string;
}

function fetchAttributes(nextEvent: () => SaxEvent, name: string): Attributes {
    const tag = (fetchNextEvent(nextEvent, 'openTag') as OpenTagEvent).tag;
    assertEquals(tag.name, name);
    return tag.attributes;

}

function expectOpenTag(nextEvent: () => SaxEvent, name: string, attributes?: Attributes): void {
    assertEquals(fetchAttributes(nextEvent, name), isUndefined(attributes) ? {} : attributes);
}

function processTextTag(nextEvent: () => SaxEvent, name: string): string {
    expectOpenTag(nextEvent, name);
    const result = (fetchNextEvent(nextEvent, 'text') as TextEvent).text;
    assertEquals((fetchNextEvent(nextEvent, 'closeTag') as CloseTagEvent).name, name);
    return result;
}

function stat(path: string): fs.Stats | undefined {
    const statResult = waitFor2((consumer: Consumer2<NodeJS.ErrnoException, fs.Stats>) => fs.stat(path, consumer));
    const err = statResult.first;
    if (err === null) {
        return statResult.second;
    }
    else {
        assertEquals(err.code, 'ENOENT');
        return undefined;
    }
}


interface MBResource {
    readonly attributes: Attributes;
    readonly next: () => SaxEvent;
}

//let requested = false;

function handleError(err: any): void {
    console.log(err);
    fail();
}


function getXml(hostName: string, path: string): () => SaxEvent {
    console.log(`https://${hostName}${path}`)
    const currentFiber = Fiber.current
    https.get({
        hostname: hostName,
        path: path,
        port: 443,
        headers: { 'user-agent': 'rasuni-musicscanner/0.0.1 ( https://musicbrainz.org/user/rasuni )' }
    }, resp => currentFiber.run(resp)).on("error", handleError);
    const resp = Fiber.yield();
    assertEquals(resp.statusCode, 200);
    const parser = sax.parser(true, {});
    resp.on('data', (chunk: any) => parser.write(chunk));
    resp.on('end', () => parser.close());
    const buffer: SaxEvent[] = [];
    let waiting: boolean = false;
    function push(event: SaxEvent): void {
        if (waiting) {
            assertEquals(buffer.length, 0);
            currentFiber.run(event);
        }
        else {
            buffer.push(event);
        }
    }
    parser.onopentag = tag => push({ type: "openTag", tag: tag as sax.Tag });
    parser.onerror = handleError;
    parser.onclosetag = name => push({ type: 'closeTag', name: name });
    parser.ontext = text => push({ type: "text", text: text });
    return (): SaxEvent => {
        const event = buffer.shift();
        if (isUndefined(event)) {
            failIf(waiting);
            waiting = true;
            const next = Fiber.yield();
            waiting = false;
            return next;
        }
        else {
            return event;
        }
    }
}

function processStringTag<T extends DynamicTask>(nextEvent: () => SaxEvent, name: string, task: T, key: keyof T, ident: string): void {
    const value = processTextTag(nextEvent, name);
    const existing: any = task[key];
    assert(existing !== value);
    task[key] = value as any;
    console.log(`${ident}${name}: ${existing} -> ${value}`);
}



function expectEqual(expectedValue: any): (actual: any) => boolean {
    return (value: any) => {
        assertEquals(value, expectedValue);
        return false;
    }
}

type Checker<T> = {
    readonly [P in keyof T]: (value: T[P]) => boolean;
}


function processObject<T>(data: T, checks: Checker<T>): boolean {
    const keys = new Set(Object.keys(data));
    for (const name in checks) {
        const checker = checks[name];
        if (checker(data[name])) {

            return false;
        }
        keys.delete(name);
    }
    assertEquals(keys.size, 0);
    return true;
}

interface NamedProcessor {
    readonly [name: string]: (actual: any) => boolean
}

function expectObject<T>(checkers: NamedProcessor): (actual: T) => boolean {
    return (actual: any) => !processObject(actual, checkers);
}
*/
interface Statement<T> {
    readonly subject: T;
    readonly predicate: T;
    readonly object: T;
}

function statement<T>(subject: T, predicate: T, object: T): Statement<T> {
    return {
        subject: subject,
        predicate: predicate,
        object: object
    };
}

interface StatementPattern {
    readonly subject?: string;
    readonly predicate?: string;
    readonly object?: string;
}

/*

function write(stream: any, subject: string, predicate: string, object: string): void {
    stream.write({ subject: subject, predicate: predicate, object: object });
}
*/

function end(stream: any): void {
    const currentFiber = Fiber.current;
    stream.on('close', () => currentFiber.run());
    stream.end();
    Fiber.yield();
}

/*
interface Updater {
    put: (subject: string, predicate: string, object: string) => void;
    updateObject: (subject: string, predicate: string, existingObject: string, newObject: string) => void;
}
*/

type Operation = "put" | "del";

interface UpdateStatement extends Statement<string> {
    operation: Operation
}

function updateStatement(action: Operation, subject: string, predicate: string, object: string): UpdateStatement {
    return {
        operation: action,
        subject: subject,
        predicate: predicate,
        object: object,
    }
}

function put(subject: string, predicate: string, object: string): UpdateStatement {
    return updateStatement('put', subject, predicate, object);
}

function updateObject(subject: string, predicate: string, existingObject: string, newObject: string): UpdateStatement[] {
    return [
        updateStatement('del', subject, predicate, existingObject),
        put(subject, predicate, newObject)
    ]
}

function throwError(err: any): void {
    if (err) {
        throw err;
    }
}

Fiber(() => {

    let executed = false;

    function defineCommand(cmdSyntax: string, description: string, options: string[], action: (...args: any[]) => void) {
        var cmd = commander.command(cmdSyntax).description(description);
        for (const option of options) {
            cmd = cmd.option(option);
        }
        cmd.action((...args: any[]) => {
            action(...args);
            executed = true;
        });
    }
    const db = levelgraph(level(path.join(__dirname, 'music-scanner')), { joinAlgorithm: 'basic' });

    function get(pattern: StatementPattern): Statement<string> | undefined {
        const getResult: Pair<any, any> = waitFor2((consumer: Consumer2<any, any>) => db.get(pattern, consumer));
        assertEquals(getResult.first, null);
        const list = getResult.second;
        const length = list.length;
        if (length === 0) {
            return undefined;
        }
        else {
            assertEquals(length, 1);
            return list[0];
        }
    }

    function getObject(subject: string, predicate: string): string | undefined {
        const statement = get({ subject: subject, predicate: predicate });
        if (isUndefined(statement)) {
            return undefined;
        }
        else {
            assertEquals(statement.subject, subject);
            assertEquals(statement.predicate, predicate);
            return statement.object;
        }
    }

    function update(changeSet: UpdateStatement[]): void {
        const putStream = db.putStream();
        const delStream = db.delStream();
        const streams = {
            put: putStream,
            del: delStream
        }
        for (const s of changeSet) {
            streams[s.operation].write(statement(s.subject, s.predicate, s.object));
        }
        end(putStream);
        end(delStream);

    }


    function processCurrent(): void {
        const currentTask = getObject('root', 'current');
        if (isUndefined(currentTask)) {
            console.log('initializing database');
            update([
                put('root', 'current', 'root'),
                put('root', 'type', 'root'),
                put('root', 'next', 'root')
            ]);
        }
        else {

            function getAttribute(name: string): string {
                const attr = getObject(currentTask as string, name);
                assertDefined(attr);
                return attr as string;
            }

            const type = getAttribute('type');

            assertEquals(type, 'root');
            console.log('root');
            const vVolume = db.v('volume');
            const path = `s/${encodeURIComponent('/Volumes/Musik')}`;
            assertEquals(waitFor2((consumer: Consumer2<any, any>) => db.search([
                statement(vVolume, 'type', 'volume'),
                statement(vVolume, 'path', path)
            ], consumer)), { first: null, second: [] });

            const prevStatement = get({ predicate: 'next', object: currentTask }) as Statement<string>;
            assertDefined(prevStatement);

            assertEquals(prevStatement.predicate, 'next');
            assertEquals(prevStatement.object, currentTask);
            const prev = prevStatement.subject;



            // todo update in
            console.log('  adding volume /Volumes/Musik')

            const volumeId = `task/${uuid()}`;

            update([
                put(volumeId, 'type', 'volume'),
                put(volumeId, 'path', path),
                put(volumeId, 'next', currentTask),
                ...updateObject(prev, 'next', currentTask, volumeId)
            ])

            const next = getAttribute('next');

            update(updateObject('root', 'current', currentTask, next));

        }

    }
    defineCommand("next", "process current task", [], processCurrent);

    defineCommand("run", "continously process all tasks until manual intervention is required", [], () => {
        for (; ;) {
            processCurrent();
        }
    });

    function getAll(subject: string | undefined, predicate: string | undefined, object: string | undefined, cb: (line: string) => void): void {
        db.getStream(statement(subject, predicate, object)).on('data', (triple: any) => {
            cb(`subject=${triple.subject} predicate=${triple.predicate} object=${triple.object}`)
        });
    }


    defineCommand("get", "retrieve triples from database", ["-s, --subject <IRI>", "-p, --predicate <IRI>", "-o, --object <IRI>"], options => {
        getAll(options.subject, options.predicate, options.object, console.log);
    });


    defineCommand("browse <uri>", "browse URI, shows triples where URI is used either as subject, predicate or object", [], uri => {
        var listed = immutable.Set();
        function browse(subject: any, predicate: any, object: any) {
            getAll(subject, predicate, object, line => {
                if (!listed.contains(line)) {
                    console.log(line);
                    listed = listed.add(line);
                }
            });
        }
        browse(uri, undefined, undefined);
        browse(undefined, uri, undefined);
        browse(undefined, undefined, uri);
    });

    function tripleCommand(commandName: string, description: string, functionName: string) {
        defineCommand(`${commandName} <subject> <predicate> <object>`, description, [], (subject, predicate, object) => db[functionName](statement(subject, predicate, object), throwError));
    }


    tripleCommand("put", "store a triple in the database", "put");

    tripleCommand("delete", "removes a triple from the database", "del");

    function wrapCallBack<T>(cb: (content: T) => void): (err: any, content: T) => void {
        return (err, content) => {
            throwError(err);
            cb(content);
        }
    }


    function specCommand(cmdName: string, description: string, specHandler: (content: any) => void) {
        defineCommand(`${cmdName} <${cmdName}spec>`, description, [], (spec) => fs.readFile(spec, 'utf8', wrapCallBack(content => specHandler(JSON.parse(content)))));
    }

    function replaceVariables(source: Statement<string>, mapper: (value: string) => any) {
        const mapVariable = (value: string) => {
            return value.startsWith('?') ? mapper(value.slice(1)) : value;
        };
        return statement(mapVariable(source.subject), mapVariable(source.predicate), mapVariable(source.object));
    }


    function search(query: Statement<string>[], handlers: any): void {
        const stream = db.searchStream(query.map((source: any) => {
            return replaceVariables(source, db.v);
        }));
        for (const name in handlers) {
            stream.on(name, handlers[name]);
        }
    }


    specCommand("update", "update the database (experimental)", query => {
        let actions: UpdateStatement[] = [];
        search(query, {
            data: (data: any) => {
                query.update.forEach((action: any) => {
                    actions.push({ operation: action.type, ...replaceVariables(action, variableName => data[variableName]) })
                })
            },
            end: () => {
                const putStream = db.putStream();
                const delStream = db.delStream();
                const streams: any = {
                    put: putStream,
                    del: delStream
                }
                actions.forEach((action: UpdateStatement) => {
                    const type = action.operation;
                    const s = statement(action.subject, action.predicate, action.object);
                    console.log(`${type} subject: ${s.subject} predicate=${s.predicate} object=${s.object}`)
                    streams[type].write(statement);
                });
                putStream.end();
                delStream.end();
                console.log('processed!')
            }
        });
    });

    specCommand("query", "queries the database", (query) => {
        search(query, {
            data: (data: any) => console.log(query.select.map((field: string) => {
                const fieldName = field.slice(1);
                return `${fieldName}: ${data[fieldName]}`
            }).join(', '))
        })
    });

    commander.parse(process.argv);
    if (!executed) {
        commander.outputHelp();
    }


    /*
    const getResult: GetResult = waitFor((consumer: Consumer<GetResult>) => db.get({ subject: 'ms:root', predicate: 'ms:current' }, (err: any, list: any) => consumer({ err: err, list: list })));
    assertEquals(getResult.err, null);
    const list = getResult.list;
    switch (list.length) {
        case 0:
            console.log('initializing database');
            const res = waitFor((consumer: Consumer<any>) => db.put([
                { subject: 'ms:root', predicate: 'ms:current', object: 'ms:root' },
                { subject: 'ms:root', predicate: 'ms:type', object: 'ms:root' },
                { subject: 'ms:root', predicate: 'ms:next', object: 'ms:root' },
            ], consumer));
            assertUndefined(res);
            break;
        case 1:
            const statement = list[0];
            assertEquals(statement.subject, 'ms:root');
            assertEquals(statement.predicate, 'ms:current');
            //assertEquals(statement.object, 'ms:root');
            const currentTask = statement.object;

            const getType: GetResult = waitFor((consumer: Consumer<GetResult>) => db.get({ subject: currentTask, predicate: 'ms:type' }, (err: any, list: any) => consumer({ err: err, list: list })));

            assertEquals(getType.err, null);
            assertEquals(getType.list, [{ subject: currentTask, predicate: 'ms:type', object: 'ms:root' }])
            console.log('root');
            const getResult1: GetResult = waitFor((consumer: Consumer<GetResult>) => db.get({ subject: 'ms:MusikServer', predicate: 'ms:root', object: 'ms:root' }, (err: any, list: any) => consumer({ err: err, list: list })));
            assertEquals(getResult1.err, null);
            assertEquals(getResult1.list, []);


            const getResult2: GetResult = waitFor((consumer: Consumer<GetResult>) => db.get({ predicate: 'ms:next', object: currentTask }, (err: any, list: any) => consumer({ err: err, list: list })));
            assertEquals(getResult2.err, null);
            const prevResults = getResult2.list
            assertEquals(prevResults.length, 1);
            const prevStatement = prevResults[0];
            assertEquals(prevStatement.predicate, 'ms:next');
            assertEquals(prevStatement.object, currentTask);
            const prev = prevStatement.subject;
            //assertEquals(prev.object, )

            const getResult3: GetResult = waitFor((consumer: Consumer<GetResult>) => db.get({ subject: currentTask, predicate: 'ms:next' }, (err: any, list: any) => consumer({ err: err, list: list })));
            assertEquals(getResult3.err, null);
            const nextResults = getResult3.list
            assertEquals(nextResults.length, 1);
            const nextStatement = nextResults[0];
            assertEquals(nextStatement.predicate, 'ms:next');
            assertEquals(nextStatement.subject, currentTask);
            const next = prevStatement.object;

            fail();


            db.put([
                { subject: 'ms:MusikServer', predicate: 'ms:root', object: 'ms:root' },
                { subject: 'ms:MusikServer', predicate: 'ms:type', object: 'ms:volume' },
                { subject: 'ms:MusikServer', predicate: 'ms:path', object: `l:s:${encodeURIComponent('/Volumes/Musik')}` },
                { subject: 'ms:MusikServer', predicate: 'ms:next', object: currentTask },
                { subject: prev, predicate: 'ms:next', object: 'ms:MusikServer' },
                { subject: 'ms:root', predicate: 'ms:current', object: next }
            ]);
            db.del([
                { subject: currentTask, predicate: 'ms:next', object: next },
                { subject: 'ms:root', predicate: 'ms:current', object: currentTask }
            ])

            //update next of prev
            //update current


            //assert (list[0])
            fail();
            break;
        case 2:
            fail();
            break;
    }
}

/*
const records: Map<string, Record> = new Map();

function setRecord(id: string, record: Record): void {
    records.set(id, record);
}

function update(id: string, content: any, loaded: string | undefined): void {
    setRecord(id, {
        type: "loaded",
        currentContent: content,
        loadedContent: loaded,
    });
}

function put(id: string, value: any): void {
    update(id, value, undefined);
}

function putTask(taskId: number, task: LinkedTask): void {
    put(`task/${taskId}`, task);
}

function deleteRecord(id: string) {
    setRecord(id, DELETED);
}

function get(id: string): any {
    const record = records.get(id);
    if (isUndefined(record)) {
        const result = waitFor2((consumer: Consumer2<any, string>) => db.get(id, consumer));
        const err = result.first;
        if (err === null) {
            const value = result.second;
            const task = JSON.parse(value);
            update(id, task, value);
            return task;
        }
        else {
            assertEquals(err.type, 'NotFoundError');
            update(id, undefined, undefined);
            return undefined;
        }
    }
    else {
        return record.type === "loaded" ? record.currentContent : undefined;
    }
}

function commit(): void {
    //getCurrentTask(db);
    let batch = db.batch();
    records.forEach((record: Record, id: string) => {
        if (record.type === "loaded") {
            const json = JSON.stringify(record.currentContent);
            if (json !== record.loadedContent) {
                batch = batch.put(id, json);
            }
        }
        else {
            batch = batch.del(id);
        }
    });
    assertUndefined(waitFor(consumer => batch.write(consumer)));
    records.clear();
}

function getTask(taskId: number): LinkedTask {
    const task = get(taskKey(taskId));
    assertDefined(task);
    return task as LinkedTask;
}

function setNext(previousTaskId: number, nextTaskId: number): void {
    getTask(previousTaskId).next = nextTaskId;
}

function getPath(taskId: number): string {
    const task = getTask(taskId);
    if (task.type === 'root') {
        return '/Volumes/Musik';
    }
    else {
        assertEquals(task.type, 'fileSystemEntry');
        const fse = task as FileSystemEntry;
        return path.join(getPath(fse.directory), fse.name);
    }
}

/*
function getRoot(): Root {
    let root: LinkedTask = get('task/0');
    if (isUndefined(root)) {
        console.log("initializing database");
        root = {
            nextId: 1,
            currentTask: 0,
            next: 0,
            previous: 0,
            type: "root",
        }
        putTask(0, root);
    }
    return root as Root;
}
*/

    /*
    function getCurrentTaskId() {
        return getRoot().currentTask;
    }
    */


    /*
    function getCurrentPath(): string {
        return getPath(getCurrentTaskId());
    }
    */

    /*
    function getCurrentTask(): LinkedTask {
        return getTask(getCurrentTaskId());
    }
    */

    /*
    function updateCurrentTask(): void {
        getRoot().currentTask = getCurrentTask().next;
    }
    */

    /*
    function moveToNextTask(): void {
        updateCurrentTask();
        commit();
    }
    */

    /*
    function adding(keyValue: string, task: DynamicTask, indent: string, log: string): AddInfo {
        const existing = get(keyValue);
        if (isUndefined(existing)) {
            console.log(`${indent}adding ${log}`);
            const root = getRoot();
            const nextId = root.nextId;
            put(keyValue, nextId);
            const currentTask = getCurrentTask();
            const previousTaskId = currentTask.previous;
            currentTask.previous = nextId;
            setNext(previousTaskId, nextId);
            const currentTaskId = getCurrentTaskId();
            putTask(nextId, {
                next: currentTaskId,
                previous: previousTaskId,
                ...(task as DynamicTask),
            });
            root.nextId = nextId + 1;
            return {
                added: true,
                id: nextId
            }
        }
        else {
            console.log(`${indent}already added ${log}`);
            return {
                added: false,
                id: existing,
            }
        }
    }
    */

    /*
    function processCurrentDirectory(): void {
        const readDirResult = waitFor2((consumer: Consumer2<NodeJS.ErrnoException, string[]>) => fs.readdir(getCurrentPath(), consumer));
        assertEquals(readDirResult.first, null);
        const files: string[] = readDirResult.second;
        failIf(files.length === 0);
        const currentTask = getCurrentTask();
 
        const directory = currentTask as Directory;
        let entries = directory.entries;
        if (isUndefined(entries)) {
            entries = {};
            directory.entries = entries;
        }
        for (const name of files) {
            if (isUndefined(entries[name])) {
                console.log(`  adding ${name}`);
                const root = getRoot();
                const nextId = root.nextId;
                const previousTaskId = currentTask.previous;
                currentTask.previous = nextId;
                setNext(previousTaskId, nextId);
                const currentTaskId = getCurrentTaskId();
                putTask(nextId, {
                    next: currentTaskId,
                    previous: previousTaskId,
                    ...({
                        directory: currentTaskId,
                        name: name,
                        type: "fileSystemEntry",
                    } as DynamicTask),
                });
                root.nextId = nextId + 1;
                entries[name] = nextId;
                directory.entries = entries;
                break;
            }
            console.log(`  already added ${name}`);
        };
        moveToNextTask();
    }
    */

    /*
    let lastMBAccess: number | undefined = undefined;
 
    function getMusicBrainzResource(type: string, mbid: string, path: string): MBResource {
        const now = Date.now();
        if (lastMBAccess !== undefined) {
            assert((now - lastMBAccess) / 1000 > 1)
        }
        lastMBAccess = now;
        const nextEvent = getXml('musicbrainz.org', path);
        expectOpenTag(nextEvent, 'metadata', {
            xmlns: "http://musicbrainz.org/ns/mmd-2.0#"
        });
        const attrs = fetchAttributes(nextEvent, type);
        assertEquals(attrs.id, mbid);
        const newAttrs: any = {};
        for (const key in attrs) {
            if (key !== 'id') {
                newAttrs[key] = attrs[key];
            }
        }
        return {
            attributes: newAttrs,
            next: nextEvent
        };
    }
 
    let done: boolean = false;
 
    function setDone(): void {
        done = true;
    }
 
    while (!done) {
        let linkedRoot: LinkedTask = get('task/0');
        if (isUndefined(linkedRoot)) {
            console.log("initializing database");
            linkedRoot = {
                nextId: 1,
                currentTask: 0,
                next: 0,
                previous: 0,
                type: "root",
            }
            putTask(0, linkedRoot);
        }
        const root = linkedRoot as Root;
 
        //const root = getRoot()
        const currentTaskId = root.currentTask;
 
 
        function getCurrentPath(): string {
            return getPath(currentTaskId);
        }
 
        function adding(name: string, task: DynamicTask): number {
            console.log(`  adding ${name}`);
            const nextId = root.nextId;
            const previousTaskId = currentTask.previous;
            currentTask.previous = nextId;
            setNext(previousTaskId, nextId);
            putTask(nextId, {
                next: currentTaskId,
                previous: previousTaskId,
                ...task,
            });
            root.nextId = nextId + 1;
            return nextId;
        }
 
        function processCurrentDirectory(): void {
            const readDirResult = waitFor2((consumer: Consumer2<NodeJS.ErrnoException, string[]>) => fs.readdir(getCurrentPath(), consumer));
            assertEquals(readDirResult.first, null);
            const files: string[] = readDirResult.second;
            failIf(files.length === 0);
            //const currentTask = getCurrentTask();
 
            const directory = currentTask as Directory;
            let entries = directory.entries;
            if (isUndefined(entries)) {
                entries = {};
                directory.entries = entries;
            }
            for (const name of files) {
                if (isUndefined(entries[name])) {
                    entries[name] = adding(name, {
                        directory: currentTaskId,
                        name: name,
                        type: "fileSystemEntry",
                    });
                    //directory.entries = entries;
                    break;
                }
                //console.log(`  already added ${name}`);
            };
            moveToNextTask();
        }
 
        const currentTask = getTask(currentTaskId);
        console.log(currentTask);
 
        function updateCurrentTask(): void {
            root.currentTask = currentTask.next;
        }
 
        function moveToNextTask(): void {
            updateCurrentTask();
            commit();
        }
 
        switch (currentTask.type) {
            case 'root':
                console.log('processing root');
                processCurrentDirectory();
                break;
            case 'fileSystemEntry':
                const p = getCurrentPath();
                console.log(`processing file system entry ${p}`);
                const stats = stat(p);
                if (stats === undefined) {
                    if (isUndefined(stat('/Volumes/Musik'))) {
                        console.error("/Volumes/Musik' is not mounted");
                        setDone();
                    }
                    else {
                        assertUndefined(currentTask.entries);
                        console.log('  entry not found -> removing task');
                        const nextTaskId = currentTask.next;
                        const next = getTask(nextTaskId);
                        const previousTaskId = currentTask.previous;
                        next.previous = previousTaskId;
 
                        setNext(previousTaskId, nextTaskId);
                        root.currentTask = nextTaskId;
                        updateCurrentTask();
                        deleteRecord(taskKey(currentTaskId));
                        commit();
 
 
                    }
                }
                else {
                    const isDirectory = stats.isDirectory();
                    if (currentTask.isDirectory === isDirectory) {
                        //console.log(`  isDirectory: ${isDirectory}`);
                        if (isDirectory) {
                            processCurrentDirectory();
                        }
                        else {
                            assert(stats.isFile());
                            const name = currentTask.name;
 
                            function unlink(): void {
                                console.log('  deleting');
                                assertEquals(waitFor((consumer: Consumer<any>) => fs.unlink(p, consumer)), null);
                            }
 
                            if (name === '.DS_Store') {
                                unlink();
                            }
                            else {
                                const extension = path.extname(name);
                                if (extension === '.flac') {
                                    //assertEquals(path.extname(name), '.flac');
                                    const promise: Promise<mm.IAudioMetadata> = mm.parseFile(p);
                                    const fiber = Fiber.current;
                                    promise.then((value: mm.IAudioMetadata) => fiber.run({ type: "metadata", metaData: value }), (err: any) => fiber.run({ type: "error", error: err }));
                                    const r = Fiber.yield();
 
                                    assertEquals(r.type, 'metadata');
                                    let metaData: mm.IAudioMetadata = r.metaData;
 
                                    function checkUpdate<T extends keyof FileSystemEntry>(name: T) {
                                        return (actual: FileSystemEntry[T]) => {
                                            const fse = currentTask as FileSystemEntry;
                                            const existing = fse[name];
                                            if (existing === actual) {
                                                return false;
                                            }
                                            else {
                                                console.log(`  ${name}: ${existing} -> ${actual}`);
                                                if (isUndefined(actual)) {
                                                    delete fse[name];
                                                }
                                                else {
                                                    fse[name] = actual;
                                                }
                                                return true;
                                            }
 
                                        }
                                    }
                                    failIf(processObject(metaData, {
                                        format: expectObject({
                                            dataformat: expectEqual('flac'),
                                            lossless: expectEqual(true),
                                            numberOfChannels: expectEqual(2),
                                            bitsPerSample: checkUpdate('bitsPerSample'),
                                            sampleRate: checkUpdate('sampleRate'),
                                            duration: checkUpdate('duration'),
                                            tagTypes: expectEqual(["vorbis"])
                                        }),
                                        native: (actual: mm.INativeTags | undefined) => {
                                            assertUndefined(actual);
                                            return false;
                                        },
                                        common: expectObject({
                                            track: expectObject({
                                                no: checkUpdate('trackNumber'),
                                                of: checkUpdate('totalTracks')
                                            }),
                                            disk: expectEqual({ no: 1, of: 1 }),
                                            barcode: checkUpdate('barcode'),
                                            title: checkUpdate('title'),
                                            releasecountry: checkUpdate('releaseCountry'),
                                            label: checkUpdate('label'),
                                            musicbrainz_albumartistid: expectObject({
                                                length: expectEqual(1),
                                                0: (value: string) => {
                                                    const keyValue = `artist/${value}`;
                                                    assertUndefined(get(keyValue));
                                                    const newTaskId = adding(`artist ${value}`, {
                                                        type: "artist",
                                                        mbid: value,
                                                        albumArtistFiles: [currentTaskId]
                                                    });
                                                    put(keyValue, newTaskId)
                                                    currentTask.albumArtist = newTaskId;
                                                    return true;
                                                }
                                            }),
                                            year: expectEqual(1991),
                                            date: expectEqual("1991-12-05"),
                                            musicbrainz_trackid: expectEqual("c863d72a-9ff4-3a4e-b3df-303a48b65825"),
                                            asin: expectEqual("B0000026GQ"),
                                            albumartistsort: expectEqual("Adam, Adolphe"),
                                            originaldate: expectEqual("1991-12-05"),
                                            conductor: expectEqual(["Michael Tilson Thomas"]),
                                            script: expectEqual("Latn"),
                                            musicbrainz_albumid: expectEqual("984d77cf-c854-4548-88db-980d6f0d624b"),
                                            releasestatus: expectEqual("official"),
                                            albumartist: expectEqual("Adolphe Adam"),
                                            acoustid_id: expectEqual("d2e18316-3caa-4ce6-bd5b-597cbb8c6874"),
                                            catalognumber: expectEqual("SK 42450"),
                                            album: expectEqual("Adolphe Adam: Music from Giselle"),
                                            musicbrainz_artistid: expectEqual(["ece590a3-ae0e-4311-bcf3-ee07cfb7b4f0"]),
                                            media: expectEqual("CD"),
                                            releasetype: expectEqual(["album"]),
                                            originalyear: expectEqual(1991),
                                            artist: expectEqual("Adolphe Adam"),
                                            musicbrainz_releasegroupid: expectEqual("17d6fe45-2813-34e1-b1bf-d4a1cdae64b1"),
                                            musicbrainz_recordingid: expectEqual("e22d63b6-c3c5-44f7-baf8-74023cd00320"),
                                            artistsort: expectEqual("Adam, Adolphe"),
                                            artists: expectEqual(["Adolphe Adam"]),
                                            picture: expectObject({
                                                length: expectEqual(1),
                                                0: expectObject({
                                                    format: expectEqual('jpg'),
                                                    data: (data: any) => {
                                                        assertEquals(data.length, 63989)
                                                        return true;
                                                    }
                                                }),
                                            }),
                                        })
                                    }));
                                }
                                else {
                                    assertEquals(extension, '.jpg')
                                    unlink();
                                }
                            }
                            moveToNextTask();
                        }
                    }
                    else {
                        console.log(`  isDirectory: ${currentTask.isDirectory} -> ${isDirectory}`);
                        currentTask.isDirectory = isDirectory;
                        moveToNextTask();
                    }
                }
                break;
            case 'recording':
                //console.log(currentTask.mbid);
                const mbRes = getMusicBrainzResource('recording', currentTask.mbid, `/ws/2/recording/${currentTask.mbid}?inc=artists+releases`);
                assertEquals(mbRes.attributes, {});
                const next = mbRes.next;
 
                processStringTag(next, 'title', currentTask, 'title', '  ');
                const length = Number(processTextTag(next, 'length')) / 1000;
                const seconds = String(length % 60);
                currentTask.length = length;
                console.log(`  length: ${Math.floor(length / 60)}:${seconds.length === 1 ? "0" + seconds : seconds}`);
                expectOpenTag(next, 'artist-credit');
                expectOpenTag(next, 'name-credit');
                const artistAttributes = fetchAttributes(next, 'artist');
                const mbid = artistAttributes.id;
                const artist = adding(`artist/${mbid}`, {
                    mbid: mbid,
                    type: "artist",
                }, '  ', `artist ${mbid}`);
                if (!artist.added) {
                    const artistData = getTask(artist.id) as Artist;
                 
                    processStringTag(next, 'name', artistData, 'name', '    ');
                    processStringTag(next, 'sort-name', artistData, 'sortName', '    ');
                    assertEquals((fetchNextEvent(next, 'closeTag') as CloseTagEvent).name, 'artist');
                    assertEquals((fetchNextEvent(next, 'closeTag') as CloseTagEvent).name, 'name-credit');
                    assertEquals((fetchNextEvent(next, 'closeTag') as CloseTagEvent).name, 'artist-credit');
                    const releaseListAttrs = fetchAttributes(next, 'release-list');
                    assertEquals(releaseListAttrs.count, "1");
                    const releaseAttrs = fetchAttributes(next, 'release');
                    const mbid = releaseAttrs.id;
                    const release = adding(`release/${mbid}`, {
                        mbid: mbid,
                        type: "release",
                    }, '  ', `release ${mbid}`);
                    assert(release.added);
                }
                moveToNextTask();
                break;
            case 'artist':
                const now = Date.now();
                assertUndefined(lastMBAccess);
                lastMBAccess = now;
                const nextEvent = getXml('musicbrainz.org', `/ws/2/artist/${currentTask.mbid}`);
 
                function nextOpen(name: string, attributes: Attributes): void {
                    assertEquals(nextEvent(), {
                        type: 'openTag',
                        tag: {
                            name: name,
                            attributes: attributes,
                            isSelfClosing: false
                        }
                    });
                }
                nextOpen('metadata', {
                    xmlns: 'http://musicbrainz.org/ns/mmd-2.0#'
                });
                nextOpen('artist', {
                    id: "043a40c7-fb90-42f7-89a8-077a8ff61db6",
                    type: "Person",
                    "type-id": "b6e035f4-3ce9-331c-97df-83397230b0df"
                });
                nextOpen('name', {});
                assertEquals(nextEvent(), {
                    type: 'text',
                    text: 'Cecilia Bartoli'
                });
                assertEquals(nextEvent(), {
                    type: 'closeTag',
                    name: 'name'
                });
                nextOpen('sort-name', {});
                assertEquals(nextEvent(), {
                    type: 'text',
                    text: 'Bartoli, Cecilia'
                });
                assertEquals(nextEvent(), {
                    type: 'closeTag',
                    name: 'sort-name'
                });
                nextOpen('isni-list', {});
                for (; ;) {
                    const ne = nextEvent();
                    if (ne.type === 'closeTag') {
                        assertEquals(ne.name, 'isni-list');
                        fail();
                        break;
                    }
                    assertEquals(ne, {
                        type: 'openTag',
                        tag: {
                            name: 'isni',
                            attributes: {},
                            isSelfClosing: false
                        }
                    });
                    if (!processObject(nextEvent(), {
                        type: expectEqual('text'),
                        text: (value: string) => {
                            const keyValue = `isni/${value}`;
                            assertUndefined(get(keyValue));
                            const newTaskId = adding(`isni ${value}`, {
                                type: "isni",
                                isni: value,
                                artist: currentTaskId
                            });
                            put(keyValue, newTaskId);
                            assertUndefined(currentTask.isniList);
                            currentTask.isniList = [newTaskId];
                            return true;
                        }
                    })) {
                        moveToNextTask();
                        break;
                    }
                    assertEquals(nextEvent(), {
                        type: 'closeTag',
                        name: 'isni'
                    });
                }
                //fail();
                break;
            case 'acoustid':
                {
                    const acoustid = currentTask.acoustid;
                    const nextEvent = getXml('api.acoustid.org', `/v2/lookup?client=2enkIyWW&format=xml&trackid=${acoustid}`)
                    assertEquals((fetchNextEvent(nextEvent, 'text') as TextEvent).text, '\n');
                    expectOpenTag(nextEvent, 'response');
                    assertEquals(processTextTag(nextEvent, 'status'), 'ok');
                    expectOpenTag(nextEvent, 'results');
                    expectOpenTag(nextEvent, 'result');
                    /*
                    expectOpenTag(nextEvent, 'recordings');
                    expectOpenTag(nextEvent, 'recording');
                    const len = Number(processTextTag(nextEvent, 'duration'));
                    const title = processTextTag(nextEvent, 'title');
                    const id = processTextTag(nextEvent, 'id');
                    const rec = adding(`recording/${id}`, {
                        mbid: id,
                        type: "recording",
                    }, '  ', `recording ${id}`);
                    failIf(rec.added);
                    const recordingId = rec.id;
                    currentTask.recording = recordingId;
                    const seconds = String(len % 60);
                    const recording = getTask(recordingId) as Recording;
                    recording.acoustId = currentTaskId;
                 
                    recording.length = len;
                    console.log(`    title: ${title}`);
                    recording.title = title;
                    console.log(`    length: ${Math.floor(len / 60)}:${seconds.length === 1 ? "0" + seconds : seconds}`);
                 
                    expectOpenTag(nextEvent, 'artists');
                    expectOpenTag(nextEvent, 'artist');
                    const artistId = processTextTag(nextEvent, 'id');
                    const artistRec = adding(`artist/${artistId}`, {
                        mbid: id,
                        type: "artist",
                    }, '    ', `artist ${artistId}`);
                    failIf(artistRec.added);
                    const aid = artistRec.id;
                    recording.artist = aid;
                    const artist = getTask(artistRec.id) as Artist;
                    artist.recording = aid;
                    processStringTag(nextEvent, 'name', artist, 'name', '      ');
                 
                    assertEquals((fetchNextEvent(nextEvent, 'closeTag') as CloseTagEvent).name, 'artist');
                    assertEquals((fetchNextEvent(nextEvent, 'closeTag') as CloseTagEvent).name, 'artists');
                    assertEquals((fetchNextEvent(nextEvent, 'closeTag') as CloseTagEvent).name, 'recording');
                    assertEquals((fetchNextEvent(nextEvent, 'closeTag') as CloseTagEvent).name, 'recordings');
 
                    const score = Number(processTextTag(nextEvent, 'score'));
                    //console.log(`  score: ${score}`);
 
                    assert(currentTask.score !== score);
                    //currentTask.score = score;
                    console.log(`  score: ${currentTask.score} -> ${score}`);
                    currentTask.score = score;
                    if (currentTask.score !== score) {
                 
                    }                  
                    currentTask.score = score;
                 
                    assertEquals(processTextTag(nextEvent, 'id'), acoustid);
                    assertEquals((fetchNextEvent(nextEvent, 'closeTag') as CloseTagEvent).name, 'result');
                    assertEquals((fetchNextEvent(nextEvent, 'closeTag') as CloseTagEvent).name, 'results');
                    assertEquals((fetchNextEvent(nextEvent, 'closeTag') as CloseTagEvent).name, 'response');
                    //fail();
                    moveToNextTask();
                }
                break;
            case 'isni':
                console.log(`http://isni.org/isni/${currentTask.isni}`);
                assertDefined(currentTask.artist);
                moveToNextTask();
                setDone();
                break;
            default:
                fail();
        }
    }
    */
}).run();