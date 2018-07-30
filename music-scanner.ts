import * as assertorig from 'assert';
import * as Fiber from 'fibers';
const level = require('level');
const levelgraph = require('levelgraph');
import * as path from 'path';
import * as uuid from 'uuid/v4';
import * as commander from 'commander';
import * as immutable from 'immutable';
import * as fs from 'fs';
import * as rimraf from 'rimraf';
import * as mm from 'music-metadata';
import * as https from 'https';
import * as sax from 'sax';

commander.version('0.0.1').description('music scanner command line tool');


/*

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


//type Consumer<T> = (result: T) => void;

function getRunner(): (value?: any) => void {
    const currentFiber = Fiber.current;
    return (value?: any) => currentFiber.run(value);
}

const yieldValue = Fiber.yield;

function waitFor<R>(asyncFunction: (consumer: (result: R) => void) => void): R {
    asyncFunction(getRunner());
    return yieldValue();
}

interface Pair<F, S> {
    readonly first: F,
    readonly second: S
}

type BiConsumer<F, S> = (first: F, second: S) => void;

function waitFor2<F, S>(asyncFunction: (consumer: BiConsumer<F, S>) => void): Pair<F, S> {
    return waitFor((consumer: (result: Pair<F, S>) => void) => asyncFunction((first: F, second: S) => consumer({
        first: first,
        second: second
    })));
}


function fail(): never {
    debugger;
    return assertorig.fail("failure");
}

function failIf(condition: boolean): void {
    if (condition) {
        fail();
    }
}

function isNull(value: any): value is null {
    return value === null;
}

function assert(condition: boolean): void {
    failIf(!condition);
}

function assertSame(actual: any, expected: any) {
    assert(actual === expected);
}

function assertType(value: any, expectedType: string) {
    assertSame(typeof value, expectedType);
}

function assertObject(value: any) {
    assertType(value, 'object');
}

interface Dictionary<T> {
    readonly [name: string]: T;
}

interface CompareValues<T> {
    actual: any,
    expected: T
}

function* compareObjects<T>(actual: Dictionary<any>, expected: Dictionary<T>): IterableIterator<CompareValues<T>> {
    for (const key of new Set([
        ...Object.keys(actual),
        ...Object.keys(expected)
    ]).values()) {
        yield {
            actual: actual[key],
            expected: expected[key]
        }
    }
}

function assertEquals(actual: any, expected: any): void {
    if (actual !== expected) {
        assertObject(actual);
        assertObject(expected);
        const expectedIsNull = isNull(expected);
        if (isNull(actual)) {
            assert(expectedIsNull);
        }
        else {
            failIf(expectedIsNull);
            for (const compareValue of compareObjects(actual, expected)) {
                assertEquals(compareValue.actual, compareValue.expected);
            }
        }
    }
}


function isUndefined(value: any): value is undefined {
    return value === undefined;
}

function assertUndefined(value: any): void {
    assert(isUndefined(value));
}
/*
 
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
    subject: T;
    predicate: T;
    object: T;
}

function statement<T>(subject: T, predicate: T, object: T): Statement<T> {
    return {
        subject: subject,
        predicate: predicate,
        object: object
    };
}

type StatementPattern = Partial<Statement<string>>;

/*
 
function write(stream: any, subject: string, predicate: string, object: string): void {
    stream.write({ subject: subject, predicate: predicate, object: object });
}
*/


/*
function end(stream: any): void {
    stream.on('close', getRunner());
    stream.end();
    Fiber.yield();
}
*/
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

function del(subject: string, predicate: string, object: string): UpdateStatement {
    return updateStatement('del', subject, predicate, object);
}


function decodeStringLiteral(stringLiteral: string) {
    const segments = stringLiteral.split('/');
    assertEquals(segments.length, 2);
    assertEquals(segments[0], 's');
    return decodeURIComponent(segments[1]);
}

function prepareStream(stream: any): void {

    function on(event: string, handler: any): void {
        stream.on(event, handler);
    }

    on('error', fail);
    const run = getRunner();
    on('data', run);
    on('end', run);
}

function streamOpt<T>(stream: any, onEmpty: () => T, onData: (data: any) => T): T {
    prepareStream(stream);
    const data = yieldValue();
    if (isUndefined(data)) {
        return onEmpty();
    }
    else {
        assertUndefined(yieldValue());
        return onData(data);
    }
}

function logError(message: string): void {
    console.error(message);
}

function volumeNotMounted(): void {
    logError('Volume not mounted. Please mount!');
}

const join = path.join;

const dbPath = join(__dirname, 'music-scanner');

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


const db = levelgraph(level(dbPath), { joinAlgorithm: 'basic' });

function get<T>(pattern: StatementPattern, onEmpty: () => T, onStatement: (statement: Statement<string>) => T): T {
    return streamOpt(db.getStream(pattern), onEmpty, onStatement);
}

//type Direction = "out" | "in";

function navigate<T>(source: string, predicate: string, isOutDirection: boolean, notFound: () => T, found: (target: string) => T): T {

    const pattern: StatementPattern = { predicate: predicate };
    const sourceKey = isOutDirection ? 'subject' : 'object';
    pattern[sourceKey] = source;
    return get(pattern, notFound, statement => {

        function verify(key: keyof Statement<string>, expected: string) {
            assertEquals(statement[key], expected);
        }

        verify(sourceKey, source);
        verify('predicate', predicate);

        return found(statement[isOutDirection ? 'object' : 'subject']);
    });
}

function getObject<T>(subject: string, predicate: string, notFound: () => T, found: (object: string) => T): T {
    return navigate(subject, predicate, true, notFound, found);
    /*
    return get({ subject: subject, predicate: predicate }, notFound, statement => {
 
        function verify(key: keyof Statement<string>, expected: string) {
            assertEquals(statement[key], expected);
        }
 
        verify('subject', subject);
        verify('predicate', predicate);
 
        return found(statement.object);
    });
    */
}


function getProperty(subject: string, name: string): string {
    return getObject(subject, name, fail, obj => obj);
}

function isEmpty(array: any[]): boolean {
    return array.length === 0;
}

function persist(type: Operation, statements: Statement<string>[]): void {
    if (!isEmpty(statements)) {
        assertUndefined(waitFor(callback => db[type](statements, callback)));
    }
}

type Predicate<T> = (value: T) => boolean;

type Matcher<T> = {
    readonly [P in keyof T]: Predicate<T[P]>;
};

function searchMatch<T, R>(object: T, checkers: Matcher<T>, found: () => R, notFound: () => R): R {
    for (const compareValue of compareObjects(object, checkers)) {
        const checker = compareValue.expected as Predicate<any>;
        assertType(checker, 'function');
        if (checker(compareValue.actual)) {
            return found();
        }
    }
    return notFound();
}

function expectEquals(expected: any): Predicate<any> {
    return actual => {
        assertEquals(actual, expected);
        return false;
    }
}


function update(changeSet: UpdateStatement[]): void {
    const transaction: Record<Operation, Statement<string>[]> = {
        put: [],
        del: []
    };
    changeSet.forEach(s => transaction[s.operation].push(statement(s.subject, s.predicate, s.object)));

    function store(operation: Operation): void {
        persist(operation, transaction[operation]);
    }

    store('del');
    store('put');

}

function matchObject<T>(checkers: Matcher<T>): Predicate<T> {
    return (object: T) => {
        return searchMatch(object, checkers, () => true, () => false);
        //return false;
    }
}

const expectJpgImage = matchObject({
    format: expectEquals("jpg"),
    data: () => false
});

let lastAccessed: number | undefined = undefined;

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

interface Attributes {
    readonly [key: string]: string;
}


function expectEvent(nextEvent: () => SaxEvent, expected: SaxEvent) {
    assertEquals(nextEvent(), expected);
}

function processInner(inner: () => void, nextEvent: () => SaxEvent, name: string): void {
    inner();
    expectEvent(nextEvent, {
        type: 'closeTag',
        name: name
    })
}

function onNoMatch<T>(value: T, matchers: Matcher<T>, notFound: () => void): void {
    searchMatch(value, matchers, () => { }, notFound);
}


function* enumOptional<T>(value: string | undefined, provideMapped: () => T) {
    if (value !== undefined) {
        yield provideMapped();
    }
}

function matchTag(nextEvent: () => SaxEvent, name: string, attributes: Predicate<any>, inner: () => void) {
    onNoMatch(nextEvent(), {
        type: expectEquals('openTag'),
        tag: matchObject({
            name: expectEquals(name),
            attributes: attributes,
            isSelfClosing: expectEquals(false)
        })
    }, () => processInner(inner, nextEvent, name))
}


function expectTag(nextEvent: () => SaxEvent, name: string, attributes: Attributes, inner: () => void): void {
    expectEvent(nextEvent, {
        type: 'openTag',
        tag: {
            name: name,
            attributes: attributes,
            isSelfClosing: false
        }
    });
    processInner(inner, nextEvent, name);
}


function expectTextTag(nextEvent: () => SaxEvent, name: string, attributes: Attributes, value: string): void {
    expectTag(nextEvent, name, attributes, () => expectEvent(nextEvent, {
        type: 'text',
        text: value
    }));
}

function expectPlainTextTag(nextEvent: () => SaxEvent, name: string, value: string): void {
    expectTextTag(nextEvent, name, {}, value);
}

function nameTags(nextEvent: () => SaxEvent, name: string) {
    /*
    onNoMatch<TextEvent>(nextEvent() as TextEvent, {
        type: expectEquals('text'),
        text: name,
    }, () => {
        expectPlainTextTag(nextEvent, 'sort-name', sortName);
        others();
    })
    */
    expectPlainTextTag(nextEvent, 'name', name);
    expectPlainTextTag(nextEvent, 'sort-name', name);
}

function expectNamed(nextEvent: () => SaxEvent, tagName: string, mbid: string, additionalAttributes: Attributes, name: string, additionalTags: () => void) {
    expectTag(nextEvent, tagName, {
        id: mbid,
        ...additionalAttributes
    }, () => {
        nameTags(nextEvent, name);
        additionalTags();
    });
}

function expectSimpleTag(nextEvent: () => SaxEvent, name: string, inner: () => void) {
    expectTag(nextEvent, name, {}, inner);
}

function expectIsoList(nextEvent: () => SaxEvent, id: string, code: string) {
    expectSimpleTag(nextEvent, `iso-3166-${id}-code-list`, () => expectPlainTextTag(nextEvent, `iso-3166-${id}-code`, code));
}

function expectIsoList1(nextEvent: () => SaxEvent, code: string) {
    expectIsoList(nextEvent, '1', code);
}


function expectUSIsoList(nextEvent: () => SaxEvent) {
    expectIsoList1(nextEvent, 'US');
}

function expectAreaRaw(nextEvent: () => SaxEvent, tagName: string, mbid: string, name: string, additionalTags: () => void) {
    expectNamed(nextEvent, tagName, mbid, {}, name, additionalTags);
}

function expectArea(nextEvent: () => SaxEvent, mbid: string, name: string, additionalTags: () => void) {
    expectNamed(nextEvent, 'area', mbid, {}, name, additionalTags);
}


function id(mbid: string): Attributes {
    return {
        id: mbid
    }
}


function processCurrent(): boolean {
    return getObject('root', 'current', () => {
        console.log('initializing database');

        function link(predicate: string): Statement<string> {
            return statement('root', predicate, 'root');
        }

        persist('put', [link('current'), link('type'), link('next')]);

        return true;
    }, currentTask => {

        function getPropertyFromCurrent(name: string): string {
            return getProperty(currentTask, name);
        }

        function updateObjectFromCurrent(subject: string, predicate: string, newObject: string): UpdateStatement[] {
            return [
                del(subject, predicate, currentTask),
                put(subject, predicate, newObject)
            ]
        }

        function appendToPrev(taskId: string): UpdateStatement[] {
            return navigate(currentTask, 'next', false, fail, subject => updateObjectFromCurrent(subject, 'next', taskId));
        }

        function setCurrent(newCurrent: string): UpdateStatement[] {
            return updateObjectFromCurrent('root', 'current', newCurrent);
        }

        function moveToNextStatements(): UpdateStatement[] {
            return setCurrent(getPropertyFromCurrent('next'));
        }

        function moveToNext(): void {
            update(moveToNextStatements());
        }


        function remove(message: string, removeMethod: 'unlink' | 'rmdir', path: string): void {
            console.log(`  ${message}`);
            assertSame(waitFor(cb => fs[removeMethod](path, cb)), null);
            moveToNext();
        }

        function enqueueTask<T>(name: string, type: string, namePredicate: string, parentPredicate: string | undefined, linkPredicate: string | undefined,/*additionalAttributes: (add: BiConsumer<string, string>) => void,*/ enqueued: T, alreadyAdded: () => T): T {

            const nameObject = `s/${encodeURIComponent(name)}`;


            function mapAttributeValues<S, T>(mapper: (subject: S, predicate: string, object: string) => T, subject: S): T[] {

                function map(predicate: string, object: string): T {
                    return mapper(subject, predicate, object);
                }

                return [
                    map('type', type),
                    map(namePredicate, nameObject),
                    ...enumOptional(parentPredicate, () => map(parentPredicate as string, currentTask))
                ]
            }

            return streamOpt(db.searchStream(mapAttributeValues(statement, db.v('s'))), () => {
                console.log(`  adding ${type} ${name}`);
                const taskId = `task/${uuid()}`;
                update([
                    ...mapAttributeValues(put, taskId),
                    put(taskId, 'next', currentTask),
                    ...appendToPrev(taskId),
                    ...enumOptional(linkPredicate, () => put(currentTask, linkPredicate as string, taskId))
                ]);
                // needs a second update, because the attribute 'next' might have been set just before
                // when appending a task to prev.
                moveToNext();
                return enqueued;
            }, alreadyAdded);
        }

        function enqueueTasks(items: string[], type: string, predicate: string, parentPredicate: string | undefined /*additionalAttribute: (add: BiConsumer<string, string>) => void*/): void {
            if (isUndefined(items.find(name => enqueueTask(name, type, predicate, parentPredicate, undefined, true, () => false)))) {
                console.log('  completed');
                moveToNext();
            }
        }


        /*
        function getStringProperty(predicate: string) {
            return decodeStringLiteral(getPropertyFromCurrent(predicate));
        }
        */

        /*
        function delCurrent(predicate: string, object: string): UpdateStatement {
            return del(currentTask, predicate, object);
        }
        */

        function stat<T>(path: string, success: (stats: fs.Stats) => T, missing: () => T): T {
            const result: Pair<NodeJS.ErrnoException, fs.Stats> = waitFor2((consumer: (first: NodeJS.ErrnoException, second: fs.Stats) => void) => fs.stat(path, consumer));
            const err = result.first;
            if (isNull(err)) {
                return success(result.second);
            }
            else {
                assertSame(err.code, 'ENOENT');
                return missing();
            }
        }

        function assertMissing(pattern: StatementPattern): void {
            get(pattern, () => { }, fail);
        }


        function processDirectory(path: string): boolean {
            const result: Pair<object, any> = waitFor2(consumer => fs.readdir(path, consumer));
            assertEquals(result.first, null);
            const files = result.second;
            //const files = wait2Success((consumer: (first: NodeJS.ErrnoException, second: string[]) => void) => fs.readdir(path, consumer));
            if (isEmpty(files)) {
                remove('delete empty directory', 'rmdir', path);
            }
            else {
                enqueueTasks(files, 'fileSystemEntry', 'name', 'directory' /*add => add('directory', currentTask)*/);
            }
            return true;
        }


        function updateLiteralProperty(predicate: string, value: string, literalTag: string, alreadyUpdated: () => boolean): boolean {
            function literal(): string {
                return `${literalTag}/${encodeURIComponent(value)}`
            }
            return getObject(currentTask, predicate, () => {
                console.log(`  ${predicate}: undefined --> ${value}`);
                update([
                    put(currentTask, predicate, literal()),
                    ...moveToNextStatements(),
                ]);
                return true;
            }, (object: string) => {
                assertEquals(object, literal());
                return alreadyUpdated();
            })
        }

        function updateEntryType(bValue: string, alreadyUpdated: () => boolean): boolean {
            return updateLiteralProperty('isDirectory', bValue, 'b', alreadyUpdated);
        }


        const type = getPropertyFromCurrent('type');

        function processFileSystemPath<T>(path: string, directory: () => T, file: () => T, missing: () => T): T {
            console.log(`processing ${type} ${path}`);
            return stat(path, stat => (stat.isDirectory() ? directory : file)(), missing);
        }

        function enqueueMBTask<T>(mbid: string, resource: string, linkPredicate: string | undefined, enqueued: T, alreadyExists: () => T): T {
            return enqueueTask(mbid, `mb:${resource}`, 'mb:mbid', undefined, linkPredicate, enqueued, alreadyExists);
        }

        function enqueueMBResourceTask(mbid: string, resource: string, found: () => void): void {
            enqueueMBTask(mbid, resource, undefined, undefined, found);
        }

        function getStringProperty(name: string) {
            return decodeStringLiteral(getPropertyFromCurrent(name));
        }



        function processMBResource(type: string, inner: (nextEvent: () => SaxEvent) => void, inc: string[], extraAttributes: Matcher<Attributes>): void {
            if (lastAccessed === undefined) {
                lastAccessed = Date.now();
            }
            else {
                const now = Date.now();
                assert(now - lastAccessed <= 1000);
                //setTimeout(resolve, ms);
                lastAccessed = now;
            }
            lastAccessed = Date.now();
            const mbid = getStringProperty('mb:mbid');
            //const incString = inc.length === 0 ? '' : '?inc=' + inc.join('+');
            const resourcePath = `/ws/2/${type}/${mbid}${inc.length === 0 ? '' : '?inc=' + inc.join('+')}`;
            console.log(`https://musicbrainz.org${resourcePath}`)
            const run = getRunner();
            https.get({
                hostname: 'musicbrainz.org',
                path: resourcePath,
                port: 443,
                headers: { 'user-agent': 'rasuni-musicscanner/0.0.1 ( https://musicbrainz.org/user/rasuni )' }
            }, run).on("error", fail);
            const resp = yieldValue();
            assertEquals(resp.statusCode, 200);
            const parser = sax.parser(true, {});
            resp.on('data', (chunk: any) => parser.write(chunk));
            resp.on('end', () => parser.close());
            const buffer: SaxEvent[] = [];
            let waiting: boolean = false;
            function push(event: SaxEvent): void {
                if (waiting) {
                    assertEquals(buffer.length, 0);
                    run(event);
                }
                else {
                    buffer.push(event);
                }
            }
            parser.onopentag = tag => push({ type: "openTag", tag: tag as sax.Tag });
            parser.onerror = fail;
            parser.onclosetag = name => push({ type: 'closeTag', name: name });
            parser.ontext = text => push({ type: "text", text: text });

            function nextEvent(): SaxEvent {
                const event = buffer.shift();
                if (isUndefined(event)) {
                    failIf(waiting);
                    waiting = true;
                    const next = yieldValue();
                    waiting = false;
                    return next;
                }
                else {
                    return event;
                }
            }

            matchTag(nextEvent, 'metadata', expectEquals({
                xmlns: "http://musicbrainz.org/ns/mmd-2.0#"
            }), () => matchTag(nextEvent, type, matchObject({
                id: expectEquals(mbid),
                ...extraAttributes,
            }), () => inner(nextEvent)));

        }

        function processMBNamedResource(type: string, subType: Predicate<string>, typeId: Predicate<string>, name: string, additionalTags: (nextEvent: () => SaxEvent) => void, inc: string[]): void {
            processMBResource(type, nextEvent => {
                nameTags(nextEvent, name);
                additionalTags(nextEvent);
            }, inc, {
                    type: subType,
                    'type-id': typeId
                });
        }

        function enqueueArea(mbid: string, found: () => void): void {
            enqueueMBResourceTask(mbid, 'area', found);
        }

        function checkUpdateFormatProperty(name: string): (actualValue: any) => boolean {
            return actualValue => updateLiteralProperty(`mm:format/${name}`, `${actualValue}`, 'n', fail);
        }

        switch (type) {
            case 'root':
                console.log('processing root');
                enqueueTasks(['/Volumes/Musik', '/Volumes/music', '/Volumes/Qmultimedia', '/Users/ralph.sigrist/Music/iTunes/ITunes Media/Music'], 'volume', 'path', undefined /*() => { }*/);
                return true;
            case 'volume':
                const volumePath = getStringProperty('path'); // getStringProperty('path');
                return processFileSystemPath(volumePath, () => processDirectory(volumePath), fail, () => {
                    volumeNotMounted();
                    return false;
                });
            case 'fileSystemEntry':
                const entryLiteral = getPropertyFromCurrent('name');
                let entryName = decodeStringLiteral(entryLiteral);
                let directoryId = getPropertyFromCurrent('directory');

                function getPropertyFromDirectory(name: string) {
                    return getProperty(directoryId, name);
                }

                function getStringPropertyFromDirectory(name: string) {
                    return decodeStringLiteral(getPropertyFromDirectory(name));
                }

                let ePath = entryName;

                function joinEntryPath(prepend: string): string {
                    return join(prepend, ePath);
                }

                for (; ;) {
                    const type = getPropertyFromDirectory('type');
                    if (type === 'volume') {
                        break;
                    }
                    assertEquals(type, 'fileSystemEntry');
                    ePath = joinEntryPath(getStringPropertyFromDirectory('name'));
                    directoryId = getPropertyFromDirectory('directory');
                }
                const vPath = getStringPropertyFromDirectory('path');
                const entryPath = joinEntryPath(vPath);
                return processFileSystemPath(entryPath,
                    () => updateEntryType('true', () => processDirectory(entryPath)),
                    () => updateEntryType('false', () => {
                        if (entryName === '.DS_Store') {
                            remove('deleting file', 'unlink', entryPath);
                            return true;
                        }
                        else {
                            switch (path.extname(entryName)) {
                                case '.flac':
                                case '.m4a':
                                case '.mp4':
                                    const promise: Promise<mm.IAudioMetadata> = mm.parseFile(entryPath);
                                    const fiber = Fiber.current;
                                    promise.then((value: mm.IAudioMetadata) => fiber.run({ type: "metadata", metaData: value }), (err: any) => fiber.run({ type: "error", error: err }));
                                    const r = yieldValue();

                                    assertEquals(r.type, 'metadata');
                                    let metaData: mm.IAudioMetadata = r.metaData;

                                    onNoMatch<mm.IAudioMetadata>(metaData, {
                                        format: matchObject<mm.IFormat>({
                                            dataformat: expectEquals("flac"),
                                            lossless: expectEquals(true),
                                            numberOfChannels: expectEquals(2),
                                            bitsPerSample: expectEquals(16),
                                            sampleRate: checkUpdateFormatProperty('sampleRate'), // actualValue => updateLiteralProperty('mm:format/sampleRate', `${actualValue}`, 'n', fail),
                                            //sampleRate: expectEquals(44100),
                                            duration: checkUpdateFormatProperty('duration'), //actualValue => updateLiteralProperty('mm:format/duration', `${actualValue}`, 'n', fail),
                                            //expectEquals(60.29333333333334),
                                            tagTypes: expectEquals(["vorbis"]),
                                        }),
                                        common: matchObject<mm.ICommonTagsResult>({
                                            track: expectEquals({
                                                no: 1,
                                                of: 19
                                            }),
                                            disk: expectEquals({
                                                no: 1,
                                                of: 1
                                            }),
                                            barcode: expectEquals(786127302127),
                                            producer: expectEquals(["Kevin Wales", "Harve Pierre", "Diddy", "J-Dub"]),
                                            title: expectEquals("Room 112 (intro)"),
                                            releasecountry: expectEquals("DE"),
                                            label: expectEquals("BMG"),
                                            musicbrainz_albumartistid: expectEquals(["9132d515-dc0e-4494-85ae-20f06eed14f9"]),
                                            year: expectEquals(1998),
                                            date: expectEquals("1998-11-16"),
                                            musicbrainz_trackid: expectEquals("562abb04-da87-3ab1-9866-ce8f24853701"),
                                            asin: expectEquals("B00000D9VN"),
                                            albumartistsort: expectEquals("112"),
                                            originaldate: expectEquals("1998-11-10"),
                                            language: expectEquals("eng"),
                                            script: expectEquals("Latn"),
                                            work: expectEquals("Room 112 (intro)"),
                                            musicbrainz_albumid: expectEquals("9ce47bcf-97d1-4534-b77e-b19ba6c98511"),
                                            releasestatus: expectEquals("official"),
                                            albumartist: expectEquals("112"),
                                            acoustid_id: expectEquals("91b4acf0-f50a-4087-9a65-48ae0034854b"),
                                            catalognumber: expectEquals("78612-73021-2"),
                                            album: expectEquals("Room 112"),
                                            musicbrainz_artistid: expectEquals(["9132d515-dc0e-4494-85ae-20f06eed14f9"]),
                                            media: expectEquals("CD"),
                                            releasetype: expectEquals(["album"]),
                                            mixer: expectEquals(["Michael Patterson"]),
                                            originalyear: expectEquals(1998),
                                            isrc: expectEquals("USAR19800507"),
                                            musicbrainz_releasegroupid: expectEquals("a15cf6a3-c02a-316f-8e3d-15cd8ddf95f0"),
                                            artist: expectEquals("112"),
                                            writer: expectEquals(["Michael Keith", "Quinnes Parker", "J-Dub", "Lamont Maxwell", "Slim", "Daron Jones"]),
                                            musicbrainz_workid: expectEquals("af499b43-2556-45c6-87e9-4879f5cf7abe"),
                                            musicbrainz_recordingid: expectEquals("9b871449-7109-42fe-835b-6957a006e25d"),
                                            artistsort: expectEquals("112"),
                                            artists: expectEquals(["112"]),
                                            genre: expectEquals(["R B"]),
                                            picture: matchObject<any>({
                                                0: expectJpgImage,
                                                1: expectJpgImage,
                                                2: expectJpgImage,
                                                3: expectJpgImage,
                                                4: expectJpgImage,
                                                5: expectJpgImage,
                                                6: expectJpgImage,
                                                7: expectJpgImage,
                                                8: expectJpgImage,
                                                9: expectJpgImage,
                                                10: expectJpgImage,
                                                11: expectJpgImage,
                                                12: expectJpgImage,
                                                13: expectJpgImage,
                                                14: expectJpgImage,
                                                15: expectJpgImage,
                                                16: expectJpgImage,
                                                17: expectJpgImage,
                                                18: expectJpgImage,
                                            }),
                                        }),
                                        native: expectEquals(undefined)
                                    }, () => enqueueMBResourceTask("9132d515-dc0e-4494-85ae-20f06eed14f9", "artist", () => enqueueMBResourceTask("9ce47bcf-97d1-4534-b77e-b19ba6c98511", "release", fail)));
                                    return true;

                                default:
                                    logError('unknown file type!');
                                    return false;

                            }

                        }
                    }), () => stat(vPath, () => {
                        assertMissing({ predicate: 'directory', object: currentTask });
                        const next = getPropertyFromCurrent('next');
                        prepareStream(db.getStream({ subject: currentTask }));
                        const updateStatements: UpdateStatement[] = [];
                        for (; ;) {
                            const statement = yieldValue();
                            if (isUndefined(statement)) {
                                break;
                            }
                            assertEquals(statement.subject, currentTask);
                            updateStatements.push(del(currentTask, statement.predicate, statement.object));
                        }
                        assertMissing({ predicate: currentTask });
                        console.log('  missing -> remove entry');
                        update([
                            ...appendToPrev(next),
                            ...setCurrent(next),
                            ...updateStatements
                        ]);
                        assertMissing({ object: currentTask });
                        assertMissing({ subject: currentTask });
                        return true;
                    }, () => {
                        volumeNotMounted();
                        return false;
                    }));
            case 'mb:artist':
                processMBNamedResource('artist', expectEquals('Group'), expectEquals("e431f5f6-b5d2-343d-8b36-72607fffb74b"), '112', (nextEvent: () => SaxEvent) => {

                    function localExpectArea(tagName: string, mbid: string, areaName: string, additionalTags: () => void) {
                        expectAreaRaw(nextEvent, tagName, mbid, areaName, additionalTags);
                    }

                    function localExpectTextTag(name: string, value: string) {
                        expectPlainTextTag(nextEvent, name, value);
                    }

                    localExpectTextTag('country', 'US');
                    localExpectArea('area', "489ce91b-6658-3307-9877-795b68554c98", 'United States', () => expectUSIsoList(nextEvent));
                    localExpectArea('begin-area', "26e0e534-19ea-4645-bfb3-1aa4e83a4046", 'Atlanta', () => { });
                    expectSimpleTag(nextEvent, 'life-span', () => localExpectTextTag('begin', "1996"));

                    function localExpectTag(name: string, attributes: Attributes, inner: () => void) {
                        expectTag(nextEvent, name, attributes, inner);
                    }

                    function expectRecordingCore(id: string, title: string, length: string, others: () => void): void {
                        localExpectTag('recording', {
                            id: id
                        }, () => {
                            localExpectTextTag("title", title);
                            localExpectTextTag("length", length);
                            others();
                        });
                    }

                    function expectRecording(id: string, title: string, length: string): void {
                        expectRecordingCore(id, title, length, () => { });
                    }

                    function expectRecordingExtended(id: string, title: string, length: string, tag: string, tagValue: string): void {
                        expectRecordingCore(id, title, length, () => localExpectTextTag(tag, tagValue));
                    }

                    function expectRecordingDisambiguation(id: string, title: string, length: string, tagValue: string): void {
                        expectRecordingExtended(id, title, length, "disambiguation", tagValue);
                    }


                    localExpectTag('recording-list', {
                        count: "268"
                    }, () => {
                        expectRecording("00cc81c5-0dd9-45bb-a27b-ef1d5454bf85", "All Cried Out", "277000");
                        expectRecording("1cb4f0df-21ce-4454-9346-011a5c220fec", "1's for Ya", "187000");
                        expectRecordingExtended("3d5a22ab-2a14-4206-a15b-e1f123076927", "Anywhere", "248000", "video", "true");
                        expectRecording("4094e83b-40a8-4494-b686-a9673be0a563", "Anything", "229093");
                        expectRecording("454c0f69-70a4-45e9-94ce-f207848fb118", "Anywhere", "197000");
                        expectRecording("58d482f4-7070-451b-bf0f-c5d0cd1491fa", "Anywhere (remix)", "235000")
                        expectRecording("7001506d-45cf-491a-bf76-a4da729eff1d", "Anywhere (interlude)", "70573");
                        expectRecording("8101a28e-36f5-482a-b947-773d605e96de", "Anywhere", "247213");
                        expectRecording("8540c247-8043-4001-bd2a-363f750fc98f", "Anywhere", "247493");
                        expectRecording("8e6f61af-8a46-4d5f-9b59-d31ed6b66266", "Anywhere", "182826");
                        expectRecording("91847fac-a4ff-443b-a4cb-92cc42f2ba24", "Be With You", "244973");
                        expectRecording("9c954ae6-c25e-4fd6-856b-7b39a8461e16", "Can I Touch You", "304573");
                        expectRecording("a36be457-c34e-4078-84da-24894d0e34d4", "Both of Us", "219000");
                        expectRecording("b5d0a806-3bb2-474f-828d-55806de56531", "Anywhere (Slang club remix)", "236533");
                        expectRecording("ba519c4e-6d74-4c17-9c3a-7bbb8d6e4680", "All My Love", "261866");
                        expectRecording("c1ac5a1a-09f2-41f0-b6ca-8ead1730c18b", "Anywhere (remix)", "280533");
                        expectRecording("d036d54a-639a-409d-81ac-f1293bb3c004", "All I Want Is You", "221706");
                        expectRecording("d1f1b2b5-8496-4182-91a9-5e5aeea9b291", "Call My Name", "247640");
                        expectRecordingDisambiguation("db8b99df-ed71-4ef9-ae3c-532c192226e7", "112 Intro", "76000", "Part III version");
                        expectRecording("e0f71717-2e40-4546-b0b6-15805d62acb4", "All Cried Out (edit)", "223426");
                        expectRecording("e87af726-1fa3-4804-821b-741e35cb2811", "All My Love", "283173");
                        expectRecordingDisambiguation("f8aa3a67-5368-44c5-8d9e-4433dea9fab2", "112 Intro", "132960", "original version");
                        expectRecording("fbf9e86b-3a11-4a08-b77d-1432e1e243c9", "Anywhere", "336880");
                        expectRecording("fc479b8c-ebbe-46f0-84cf-f45615f165cf", "After the Love Has Gone", "243000");
                        expectRecording("ff22df38-fe40-4487-9006-fdc6a4662fe0", "Anywhere", "244026")
                    });



                }, ['recordings']);
                enqueueArea("489ce91b-6658-3307-9877-795b68554c98", () => enqueueArea("26e0e534-19ea-4645-bfb3-1aa4e83a4046", () => enqueueMBResourceTask("00cc81c5-0dd9-45bb-a27b-ef1d5454bf85", 'recording', fail)));
                return true;
            case 'mb:area':
                processMBNamedResource('area', expectEquals('Country'), areaTypeId => enqueueMBTask(areaTypeId, `area-type`, 'mb:type', true, () => false), 'United States', (nextEvent: () => SaxEvent) => {
                    expectUSIsoList(nextEvent);

                    function localExpectTag(name: string, attributes: Attributes, inner: () => void) {
                        expectTag(nextEvent, name, attributes, inner);
                    }

                    function localExpectPlainTextTag(name: string, value: string): void {
                        expectPlainTextTag(nextEvent, name, value);
                    }

                    function expectTagTagWithCount(count: string, name: string) {
                        localExpectTag('tag', { count: count }, () => localExpectPlainTextTag('name', name));
                    }

                    function expectTagTag(name: string) {
                        expectTagTagWithCount('0', name);
                    }

                    function localExpectIsoList(id: string, code: string) {
                        expectIsoList(nextEvent, id, code);
                    }

                    function expectPartOfRaw(mbid: string, name: string, iso1List: () => void, code: string) {
                        localExpectTag('relation', {
                            'type-id': "de7cc874-8b1b-3a05-8272-f3834c968fb7",
                            type: "part of"
                        }, () => {
                            localExpectPlainTextTag('target', mbid);
                            expectArea(nextEvent, mbid, name, () => {
                                iso1List();
                                localExpectIsoList('2', code);
                            })

                        })
                    }

                    function expectPartOf(mbid: string, name: string, code: string) {
                        expectPartOfRaw(mbid, name, () => { }, code)
                    }

                    function expectPartOf2(mbid: string, name: string, code1: string, code2: string) {
                        expectPartOfRaw(mbid, name, () => localExpectIsoList('1', code1), code2);
                    }

                    localExpectTag('alias-list', {
                        count: "1"
                    }, () => expectTextTag(nextEvent, 'alias', {
                        'sort-name': "USA",
                        type: "Search hint",
                        'type-id': "7090dd35-e32e-3422-8a48-224821c2468b"
                    }, 'USA'));
                    localExpectTag('relation-list', {
                        'target-type': 'area'
                    }, () => {
                        expectPartOf('02e01cf9-b0ed-4286-ac6d-16989f92ced6', 'Virginia', 'US-VA');
                        expectPartOf('0573177b-9ff9-4643-80bc-ed2513419267', 'Ohio', 'US-OH');
                        expectPartOf('05f68b4c-10f3-49b5-b28c-260a1b707043', 'Massachusetts', 'US-MA');
                        expectPartOf('0c693f90-d889-4abe-a0e6-6aac212388e3', 'New Mexico', 'US-NM');
                        expectPartOf('10cb2ebd-1bc7-4c11-b10d-54f60c421d20', 'Wisconsin', 'US-WI');
                        expectPartOf('1462269e-911b-4db3-be41-434393484e34', 'Missouri', 'US-MO');
                        expectPartOf('1b420c08-51a5-4bdd-9b0e-cd601703d20b', 'Hawaii', 'US-HI');
                        expectPartOf('1ed51cbe-4272-4df9-9b18-44b0d4714086', 'Maryland', 'US-MD');
                        expectPartOf('2066f663-1055-4383-aaa6-08d09ec81e57', 'South Dakota', 'US-SD');
                        expectPartOf('29fa065f-a568-418c-98b9-5023f64d9312', 'Michigan', 'US-MI');
                        expectPartOf('373183af-56db-44d7-b06a-5877c02c5f01', 'Colorado', 'US-CO');
                        expectPartOf('376ea713-8f27-4ab1-818b-9cca72023382', 'Oregon', 'US-OR');
                        expectPartOf2('3906cf32-00a7-32df-93cc-4710c5f5a542', 'Puerto Rico', 'PR', 'US-PR');
                        expectPartOf('39383cce-6f78-4afe-b19a-8377995ce702', 'Washington', 'US-WA');
                        expectPartOf2('43dd540a-78cd-319f-bab9-214b5430f3f2', 'Guam', 'GU', 'US-GU');
                        expectPartOf('4ca644d9-18a6-4605-9d71-3eae8b3ab2ee', 'New Hampshire', 'US-NH');
                        expectPartOf2('4e8596fe-cbee-34ce-8b35-1f3c9bc094d6', 'United States Minor Outlying Islands', 'UM', 'US-UM');
                        expectPartOf('6fddb177-f3fc-4c30-9d49-9c7e949fe0bc', 'Mississippi', 'US-MS');
                        expectPartOf('75d8fdcf-03e9-43d9-9399-131b8e118b0b', 'Pennsylvania', 'US-PA');
                        expectPartOf('75e398a3-5f3f-4224-9cd8-0fe44715bc95', 'New York', 'US-NY');
                        expectPartOf('7a0e4090-2ab5-4a28-acef-6173e3885fa7', 'Delaware', 'US-DE');
                        expectPartOf('7deb769c-1eaa-4b7a-aecf-c395d82a1e73', 'Utah', 'US-UT');
                        expectPartOf('821b0738-e1a2-4636-82e0-b5ca8b331679', 'Alaska', 'US-AK');
                        expectPartOf('85255cb8-edb9-4a66-b23a-a5261d42c116', 'Kentucky', 'US-KY');
                        expectPartOf('8788d6c2-c779-4be5-ad47-cf0a95e0f2a0', 'Arkansas', 'US-AR');
                        expectPartOf('88772016-5866-496a-8de7-4340e922d663', 'Connecticut', 'US-CT');
                        expectPartOf('8c2196d9-b7be-4051-90d1-ac81895355f1', 'Illinois', 'US-IL');
                        expectPartOf('8c3615bc-bd11-4bf0-b237-405161aac8b7', 'Iowa', 'US-IA');
                        expectPartOf2('9a84fea2-1c1f-3908-a44a-6fa2b6fa7b26', 'Northern Mariana Islands', 'MP', 'US-MP');
                        expectPartOf('a3435b4a-f42c-404e-beee-f290f62a5e1c', 'Vermont', 'US-VT');
                        expectPartOf('a36544c1-cb40-4f44-9e0e-7a5a69e403a8', 'New Jersey', 'US-NJ');
                        expectPartOf('a5ff428a-ad62-4752-8f8d-14107c574117', 'Nebraska', 'US-NE');
                        expectPartOf('ab47b3b2-838d-463c-9907-30dcd3438d65', 'Nevada', 'US-NV');
                        expectPartOf('ae0110b6-13d4-4998-9116-5b926287aa23', 'California', 'US-CA');
                        expectPartOf('aec173a2-0f12-489e-812b-7d2c252e4b62', 'South Carolina', 'US-SC');
                        expectPartOf('af4758fa-92d7-4f49-ac74-f58d3113c7c5', 'North Dakota', 'US-ND');
                        expectPartOf('af59135f-38b5-4ea4-b4e2-dd28c5f0bad7', 'Washington, D.C.', 'US-DC');
                        expectPartOf('b8c5f945-678b-43eb-a77a-f237d7f01493', 'Rhode Island', 'US-RI');
                        expectPartOf('bb32d812-8161-44e1-8a73-7a0d4a6d3f96', 'West Virginia', 'US-WV');
                        expectPartOf('bf9353d8-da52-4fd9-8645-52b2349b4914', 'Arizona', 'US-AZ');
                        expectPartOf('c2dca60c-5a5f-43b9-8591-3d4e454cac4e', 'Wyoming', 'US-WY');
                        expectPartOf('c45232cf-5848-45d7-84ae-94755f8fe37e', 'Maine', 'US-ME');
                        expectPartOf('c747e5a9-3ac7-4dfb-888f-193ff598c62f', 'Kansas', 'US-KS');
                        expectPartOf('cc55c78b-15c9-45dd-8ff4-4a212c54eff3', 'Indiana', 'US-IN');
                        expectPartOf('cffc0190-1aa2-489f-b6f9-43b9a9e01a91', 'Alabama', 'US-AL');
                        expectPartOf('d10ba752-c9ce-4804-afc0-7ff94aa5d8d6', 'Georgia', 'US-GA');
                        expectPartOf('d2083d84-09e2-4d45-8fc0-45eed33748b5', 'Oklahoma', 'US-OK');
                        expectPartOf('d2918f1a-c51e-4a4a-ad7f-cdd88877b25f', 'Florida', 'US-FL');
                        expectPartOf('d4ab49e7-1d25-45e2-8659-b147e0ea3684', 'North Carolina', 'US-NC');
                        expectPartOf2('e228a3c1-53c0-3ec9-842b-ec1b2138e387', 'American Samoa', 'AS', 'US-AS');
                        expectPartOf('f2532a8e-276c-457a-b3d9-0a7706535178', 'Idaho', 'US-ID');
                        expectPartOf('f5ffcc03-ebf2-466a-bb11-b38c6c0c84f5', 'Minnesota', 'US-MN');
                        expectPartOf('f934c8da-e40e-4056-8f8c-212e68fdcaec', 'Texas', 'US-TX');
                        expectPartOf('f9caf2d8-9638-4b96-bc49-8462339d4b2e', 'Tennessee', 'US-TN');
                        expectPartOf('fb8840b9-ff2f-4484-8540-7112ee426ea7', 'Montana', 'US-MT');
                        expectPartOf('fc68ecf5-507e-4012-b60b-d93747a3cfa7', 'Louisiana', 'US-LA');
                    })
                    localExpectTag('tag-list', {}, () => {
                        expectTagTag('fail');
                        expectTagTag('place');
                        expectTagTagWithCount('-1', 'the tag voters have no sense of humour. vote either fail or whatever as an answer!');
                        expectTagTag('un member state');
                        expectTagTag('united states of what?');
                        expectTagTag('vote either fail or whatever as an answer! united states of what??');
                        expectTagTag('whatever');
                    })

                }, ['aliases', 'annotation', 'tags', 'ratings', 'area-rels']);
                enqueueArea('02e01cf9-b0ed-4286-ac6d-16989f92ced6', fail);
                return true;
            case 'mb:release':

                processMBResource('release', nextEvent => {

                    function localExpectPlainTextTag(name: string, value: string): void {
                        expectPlainTextTag(nextEvent, name, value);
                    }

                    function localExpectTextAndEnum(name: string, value: string, enumName: string, mbid: string, enumValue: string): void {
                        localExpectPlainTextTag(name, value);
                        expectTextTag(nextEvent, enumName, id(mbid), enumValue);

                    }
                    localExpectTextAndEnum('title', 'Room 112', 'status', "4e304316-386d-3409-af2e-78857eec5cfe", 'Official');
                    localExpectTextAndEnum('quality', 'normal', 'packaging', "ec27701a-4a22-37f4-bfac-6616e0f9750a", 'Jewel Case');
                    function localExpectSimpleTag(name: string, inner: () => void): void {
                        expectSimpleTag(nextEvent, name, inner);
                    }
                    localExpectSimpleTag('text-representation', () => {
                        localExpectPlainTextTag('language', 'eng');
                        localExpectPlainTextTag('script', 'Latn');
                    });

                    function expectDate() {
                        localExpectPlainTextTag('date', '1998-11-16');
                    }

                    expectDate(); //localExpectPlainTextTag('date', '1998-11-16');
                    localExpectPlainTextTag('country', 'DE');
                    expectTag(nextEvent, 'release-event-list', { count: '1' }, () => localExpectSimpleTag('release-event', () => {
                        expectDate();
                        expectArea(nextEvent, '85752fda-13c4-31a3-bee5-0e5cb1f51dad', 'Germany', () => expectIsoList1(nextEvent, 'DE'))
                    }));

                    function expectTrue(name: string): void {
                        localExpectPlainTextTag(name, 'true');
                    }

                    localExpectPlainTextTag('barcode', '786127302127');
                    localExpectPlainTextTag('asin', 'B00000D9VN');
                    localExpectSimpleTag('cover-art-archive', () => {
                        expectTrue('artwork');
                        localExpectPlainTextTag('count', '19');
                        expectTrue('front');
                        expectTrue('back');
                    })
                    //fail();
                    /*
                    expectPlainTextTag(nextEvent, 'sort-name', name);
                    additionalTags();
                    */
                }, [], {});
                enqueueArea('85752fda-13c4-31a3-bee5-0e5cb1f51dad', fail);
                return true;
            default:
                fail();
                return false;
        }
    });
}

defineCommand("next", "process current task", [], processCurrent);

defineCommand("run", "continously process all tasks until manual intervention is required", [], () => {
    while (processCurrent()) {
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
    defineCommand(`${commandName} <subject> <predicate> <object>`, description, [], (subject, predicate, object) => assertUndefined(waitFor(consumer => db[functionName](statement(subject, predicate, object), consumer))));
}


tripleCommand("put", "store a triple in the database", "put");

tripleCommand("delete", "removes a triple from the database", "del");


function specCommand(cmdName: string, description: string, specHandler: (content: any) => void) {
    defineCommand(`${cmdName} <${cmdName}spec>`, description, [], (spec) => {
        const res: Pair<NodeJS.ErrnoException, string> = waitFor2(callback => fs.readFile(spec, 'utf8', callback));
        assertUndefined(res.first);
        specHandler(JSON.parse(res.second));
    })
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


specCommand("query", "queries the database", (query) => {
    search(query, {
        data: (data: any) => console.log(query.select.map((field: string) => {
            const fieldName = field.slice(1);
            return `${fieldName}: ${data[fieldName]}`
        }).join(', '))
    })
});


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


defineCommand("purge", "removes all triples from the database, empties the database", [], () => {
    db.close();
    assertUndefined(waitFor(callback => rimraf(dbPath, callback)));
});


Fiber(() => {

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