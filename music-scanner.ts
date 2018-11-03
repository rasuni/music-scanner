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
import * as readline from 'readline';
import opn = require('opn');

commander.version('0.0.1').description('music-scanner command line tool');



type Consumer<T> = (value: T) => void;

function getRunner(): (value?: any) => void {
    const currentFiber = Fiber.current;
    return (value?: any) => currentFiber.run(value);
}

const yieldValue = Fiber.yield;

function waitFor<R>(asyncFunction: (consumer: Consumer<R>) => void): R {
    asyncFunction(getRunner());
    return yieldValue();
}

interface Pair<F, S> {
    readonly first: F,
    readonly second: S
}

type BiConsumer<F, S> = (first: F, second: S) => void;

function waitFor2<F, S>(asyncFunction: (consumer: BiConsumer<F, S>) => void): Pair<F, S> {
    return waitFor((consumer: Consumer<Pair<F, S>>) => asyncFunction((first: F, second: S) => consumer({
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

    function compareValue(key: string): CompareValues<T> {
        return {
            actual: actual[key],
            expected: expected[key]
        }
    }

    const visited = new Set();
    for (const key in actual) {
        yield compareValue(key);
        visited.add(key);
    }
    for (const key in expected) {
        if (!visited.has(key)) {
            yield compareValue(key)
        }
    }

}

function assertEquals(actual: any, expected: any): void {
    //console.log(`assertEquals (${actual}, ${expected})`)
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

function decodeLiteral(literal: string, prefix: string) {
    const segments = literal.split('/');
    assertEquals(segments.length, 2);
    assertEquals(segments[0], prefix);
    return decodeURIComponent(segments[1]);
}


function decodeStringLiteral(stringLiteral: string) {
    return decodeLiteral(stringLiteral, 's')
}

function makeBlockingQueue<T>(assignProducers: Consumer<Consumer<T>>): Provider<T> {
    const buffer: T[] = [];
    let waiting: boolean = false;
    const run = getRunner();
    assignProducers((item: T) => {
        if (waiting) {
            assertEquals(buffer.length, 0);
            run(item);
        }
        else {
            buffer.push(item);
        }
    });

    return () => {
        if (buffer.length == 0) {
            failIf(waiting);
            waiting = true;
            const next = yieldValue();
            waiting = false;
            return next;
        }
        else {
            return buffer.shift();
        }
    }

}

function makeBlockingStream<T>(on: (event: string, consumer: Consumer<T>) => void): Provider<T> {

    return makeBlockingQueue((push: Consumer<any>) => {
        on('data', push);
        on('end', push);
    });

}


function prepareDBStream(stream: any): Provider<any> {

    function on(event: string, handler: any): void {
        stream.on(event, handler);
    }
    on('error', fail);

    return makeBlockingStream(on);
}

function streamOpt<T>(stream: any, onEmpty: () => T, onData: (data: any) => T): T {
    const next = prepareDBStream(stream);
    const data = next();
    if (isUndefined(data)) {
        return onEmpty();
    }
    else {
        assertUndefined(next());
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


function mapDictionary<F, T>(source: Dictionary<F>, mapper: (key: string, value: F) => T): T[] {
    return Object.getOwnPropertyNames(source).map(key => mapper(key, source[key]));
}

function asString(value: any): string {
    switch (typeof value) {
        case 'boolean':
        case 'function':
        case 'number':
        case 'symbol':
            return value.toString();
        case 'object':
            return isUndefined(Object.getOwnPropertyDescriptor(value, "toString")) ? `{${mapDictionary(value, (name, vname) => `${name}:${asString(vname)}`).join(',')}}` : value.toString();
        case 'string':
            return `"${value}"`;
        case 'undefined':
            return "undefined";
    }
    fail();
    return "";
}


function expectEquals(expected: any): Predicate<any> {
    const res = (actual: any) => {
        assertEquals(actual, expected);
        return false;
    }
    res.toString = () => `expectEquals(${asString(expected)})`;
    return res;
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

const FALSE = () => false;

function matchObject<T>(checkers: Matcher<T>): Predicate<T> {
    const res = (object: T) => {
        return searchMatch(object, checkers, () => true, FALSE);
    }
    res.toString = () => `matchObject(${asString(checkers)})`;
    return res;
}

let lastAccessed: {
    [name: string]: number;
} = {};

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

function isDefined(value: any): value is boolean | number | string | object {
    return value !== undefined;
}

function* enumOptional<T>(value: string | undefined, provideMapped: () => T) {
    if (isDefined(value)) {
        yield provideMapped();
    }
}

interface SequenceNode<T> {
    readonly first: T,
    readonly rest: Sequence<T>,
    readonly toString?: () => string;
}

type Sequence<T> = SequenceNode<T> | undefined;

function sequence<T>(...members: T[]): Sequence<T> {

    function getRest(index: number): Sequence<T> {
        return members.length === index ? undefined : {
            get first(): T {
                return members[index];
            },
            get rest(): Sequence<T> {
                return getRest(index + 1);
            },
        }
    }
    return getRest(0);
}

function map<F, T>(sequence: Sequence<F>, mapper: (source: F) => Sequence<T>): Sequence<T> {


    function nextFromSequence(currentSequence: Sequence<T>, fallback: () => Sequence<T>): Sequence<T> {
        if (isUndefined(currentSequence)) {
            return fallback();
        }
        const cr = currentSequence;
        return {
            get first(): T {
                return cr.first;
            },
            get rest(): Sequence<T> {
                return nextFromSequence(cr.rest, fallback);
            },
            toString(): string {
                return `nextFromSequence(${asString(currentSequence)}, ${asString(fallback)})`;
            }
        }
    }

    function nextFromSource(sequence: Sequence<F>): Sequence<T> {
        return isUndefined(sequence) ? undefined : nextFromSequence(mapper(sequence.first), () => nextFromSource(sequence.rest));
    }

    return nextFromSource(sequence);

}

function concat<T>(...sequences: Sequence<T>[]): Sequence<T> {



    return map(sequence(...sequences), seq => seq);

}



function sequenceToString(sequence: Sequence<any>): string {
    const members: string[] = [];
    while (sequence != undefined) {
        members.push(asString(sequence.first));
        sequence = sequence.rest;
    }
    return members.join(',');
}

function append<T>(sequence: Sequence<T>, elem: T): SequenceNode<T> {


    const res = {
        get first(): T {
            return isUndefined(sequence) ? elem : sequence.first
        },

        get rest(): Sequence<T> {
            return isUndefined(sequence) ? undefined : append(sequence.rest, elem);
        },



    }
    res.toString = () => sequenceToString(res);
    return res;

}


function matchTag(name: string, attributes: Predicate<any>, inner: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {

    return {
        first: matchObject<SaxEvent>({
            type: expectEquals('openTag'),
            tag: matchObject({
                name: expectEquals(name),
                attributes: attributes,
                isSelfClosing: expectEquals(isUndefined(inner))
            })
        }),
        rest: append(
            inner,
            expectEquals({
                type: 'closeTag',
                name: name
            }),
        )

    }
}

function matchTagAndAttributes(name: string, attributes: Attributes, inner: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {
    return matchTag(name, expectEquals(attributes), inner);
}

function expectTag(name: string, attributes: Attributes, inner: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {
    return matchTagAndAttributes(name, attributes, inner);
}


function expectTextEvent(value: Predicate<string>): Sequence<Predicate<SaxEvent>> {
    return sequence(matchObject<SaxEvent>({ type: expectEquals('text'), text: value }));
}

function matchTextTag(name: string, attributes: Attributes, value: Predicate<string>): Sequence<Predicate<SaxEvent>> {
    return expectTag(name, attributes, expectTextEvent(value));
}


function matchPlainTextTag(name: string, value: Predicate<string>): Sequence<Predicate<SaxEvent>> {
    return matchTextTag(name, {}, value);
}

function expectPlainTextTag(name: string, value: string): Sequence<Predicate<SaxEvent>> {
    return matchPlainTextTag(name, expectEquals(value));
}

function nameTags(name: Predicate<string>, sortName: string): Sequence<Predicate<SaxEvent>> {
    return concat(
        matchPlainTextTag('name', name),
        expectPlainTextTag('sort-name', sortName)
    )
}

function expectEntityTag(tagName: string, mbid: string, additionalTags: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {
    return expectTag(tagName, {
        id: mbid
    }, additionalTags);
}


function expectNamed(tagName: string, mbid: string, name: string, additionalTags: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {
    return expectEntityTag(tagName, mbid, concat(
        nameTags(expectEquals(name), name),
        additionalTags
    ));
}

function expectSimpleTag(name: string, inner: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {
    return expectTag(name, {}, inner);
}

function expectIsoList(id: string, code: string): Sequence<Predicate<SaxEvent>> {
    return expectSimpleTag(`iso-3166-${id}-code-list`, expectPlainTextTag(`iso-3166-${id}-code`, code));
}

function expectIsoList1(code: string): Sequence<Predicate<SaxEvent>> {
    return expectIsoList('1', code);
}

const expectUSIsoList = expectIsoList1('US');


function expectAreaRaw(tagName: string, mbid: string, name: string, additionalTags: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {
    return expectNamed(tagName, mbid, name, additionalTags);
}

function expectArea(mbid: string, name: string, additionalTags: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {
    return expectAreaRaw('area', mbid, name, additionalTags);
}

type Provider<T> = () => T;

function getStream(pattern: StatementPattern): Provider<Statement<string>> {
    return prepareDBStream(db.getStream(pattern));
}


function expectCountry(countryCode: string): Sequence<Predicate<SaxEvent>> {
    return expectPlainTextTag('country', countryCode);
}

const expectUsCountry = expectCountry('US');

function expectLifeSpan(begin: string, remaining: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {
    return expectSimpleTag('life-span', concat(
        expectPlainTextTag('begin', begin),
        remaining
    ));
}

function expectTiteledEntity(entityType: string, id: string, title: string, others: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {
    return expectEntityTag(entityType, id, concat(
        expectPlainTextTag("title", title),
        others
    ));
}

function expectNamedEntity(tagName: string, id: string, value: string): Sequence<Predicate<SaxEvent>> {
    return expectEntityTag(tagName, id, expectTextEvent(expectEquals(value)));
}

const expectTextRepresentation = expectSimpleTag('text-representation', concat(
    expectPlainTextTag('language', 'eng'),
    expectPlainTextTag('script', 'Latn')
));


function expectDate(date: string): Sequence<Predicate<SaxEvent>> {
    return expectPlainTextTag('date', date);
}

function expectList(name: string, elements: Sequence<Predicate<SaxEvent>>[]): Sequence<Predicate<SaxEvent>> {
    return expectTag(`${name}-list`, { count: `${elements.length}` }, map(sequence(...elements), element => expectSimpleTag(name, element)));
}

function expectReleaseEventList(elements: Sequence<Predicate<SaxEvent>>[]): Sequence<Predicate<SaxEvent>> {
    return expectList('release-event', elements);
}

const expect1975 = expectDate('1975');

function expectRelease(id: string, title: string, official: Sequence<Predicate<SaxEvent>>, country: string, areaId: string, packaging: Sequence<Predicate<SaxEvent>>, barcode: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {
    return expectTiteledEntity('release', id, title, concat(
        official,
        expectPlainTextTag('quality', 'normal'),
        packaging,
        expect1975,
        expectCountry(country),
        expectReleaseEventList([concat(
            expect1975,
            expectArea(areaId, 'United States', expectUSIsoList)
        )]),
        barcode
    ));
}


const expectOfficial = expectNamedEntity('status', '4e304316-386d-3409-af2e-78857eec5cfe', 'Official');


const expectBarcode = matchTagAndAttributes('barcode', {}, undefined);

const expectCardboard = concat(
    expectNamedEntity('packaging', 'f7101ce3-0384-39ce-9fde-fbbd0044d35f', 'Cardboard/Paper Sleeve'),
    expectTextRepresentation,
)

function getCurrentTask(): string {
    return getObject('root', 'current', () => {
        console.log('initializing database');

        function link(predicate: string): Statement<string> {
            return statement('root', predicate, 'root');
        }

        persist('put', [link('current'), link('type'), link('next')]);

        return 'root';
    }, currentTask => currentTask);
}


function getPrevious(currentTask: string): string {
    return navigate(currentTask, 'next', false, fail, subject => subject);
}

function link(prev: string, next: string): UpdateStatement {
    return put(prev, 'next', next);
}

function updateObjectFromCurrent(subject: string, predicate: string, newObject: string): UpdateStatement[] {
    return [
        del(subject, predicate, getCurrentTask()),
        put(subject, predicate, newObject)
    ]
}


function appendToPrev(taskId: string): UpdateStatement[] {
    return updateObjectFromCurrent(getPrevious(getCurrentTask()), 'next', taskId);
}


function tryAddKeyedTask<T>(type: string, keys: Dictionary<string>, path: string, prefix: string, linkPredicate: string | undefined, enqueued: () => T, alreadyAdded: (found: any) => T): T {

    const currentTask = getCurrentTask();


    function mapAttributeValues<S, T>(mapper: (subject: S, predicate: string, object: string) => T, subject: S): T[] {

        return mapDictionary({
            type: type,
            ...keys
        }, (key, value) => mapper(subject, key, value));
    }

    return streamOpt(db.searchStream(mapAttributeValues(statement, db.v('s'))), () => {
        console.log(`${prefix}adding ${type} ${path}`);
        const taskId = `task/${uuid()}`;
        update([
            ...mapAttributeValues(put, taskId),
            link(taskId, currentTask),
            ...appendToPrev(taskId),
            ...enumOptional(linkPredicate, () => put(currentTask, linkPredicate as string, taskId))
        ]);
        return enqueued();
    }, alreadyAdded);
}

function encodeLiteral(literalTag: string, rawString: string): string {
    return `${literalTag}/${encodeURIComponent(rawString)}`
}

function encodeString(value: string): string {
    return encodeLiteral('s', value)
}

function tryAdd<T>(name: string, type: string, namePredicate: string, parentPredicate: string | undefined, prefix: string, linkPredicate: string | undefined, enqueued: () => T, alreadyAdded: (found: any) => T): T {


    let keys: any = {
    };
    keys[namePredicate] = encodeString(name);
    if (isDefined(parentPredicate)) {
        keys[parentPredicate] = getCurrentTask();
    }
    return tryAddKeyedTask(type, keys, name, prefix, linkPredicate, enqueued, alreadyAdded);

}

function assertDefined(value: any): void {
    failIf(isUndefined(value));
}
interface Entity {
    readonly id: string;
}
interface Area {
    id: string;
    type: string;
}

interface Artist {
    id: string;
    area: Area;
    type: string;
}

interface ArtistCredit {
    readonly artist: Artist;
}

interface Relation {
    artist?: Entity;
    work?: Entity;
}
interface Recording {
    readonly id: string;
    readonly "artist-credit": ArtistCredit[];
    relations: Relation[];
    //readonly isrcs: any[];
}

interface Track {
    readonly id: string;
    readonly recording: Recording;
    readonly title: string;
}

interface Medium {
    readonly tracks: Track[];
    readonly position: number;
}

interface Release {
    readonly id: string;
    readonly media: Medium[];
    readonly status: string;
}

interface ReleaseList {
    readonly "release-count": number;
    readonly releases: Release[];
}

interface AcoustIdRecording {
    readonly id: string;
}

interface AcoustIdTrack {
    readonly id: string;
    readonly recordings: AcoustIdRecording[];
}
interface AcoustIdMetaData {
    readonly status: string;
    readonly results: AcoustIdTrack[];
}

function processCurrent(): boolean {
    const currentTask = getCurrentTask();

    function getPropertyFromCurrent(name: string): string {
        return getProperty(currentTask, name);
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


    function enqueueTask<T>(name: string, type: string, namePredicate: string, parentPredicate: string | undefined, linkPredicate: string | undefined, enqueued: T, alreadyAdded: (id: string) => T): T {
        return tryAdd(name, type, namePredicate, parentPredicate, '  ', linkPredicate, () => {
            moveToNext();
            return enqueued;
        }, (found: any) => alreadyAdded(found.s))
    }

    function enqueueUnlinkedTask(name: string, type: string, namePredicate: string, parentPredicate: string | undefined, alreadyAdded: (id: string) => boolean): boolean {
        return enqueueTask(name, type, namePredicate, parentPredicate, undefined, true, alreadyAdded);
    }

    function enqueueNextTask<T, R>(items: T[], name: (item: T) => string, type: (item: T) => string, predicate: string, parentPredicate: string | undefined, foundResult: R, completed: () => R): R {
        for (const item of items) {
            if (enqueueUnlinkedTask(name(item), type(item), predicate, parentPredicate, FALSE)) {
                return foundResult;
            }
        }
        return completed();
    }

    function enqueueTopLevelTask(name: string, type: string, namePredicate: string, alreadyAdded: (id: string) => boolean): boolean {
        return enqueueUnlinkedTask(name, type, namePredicate, undefined, alreadyAdded);
    }

    function enqueueTypedTopLevelTask(name: string, type: string, alreadyAdded: (id: string) => boolean): boolean {
        return enqueueTopLevelTask(name, type, type, alreadyAdded);
    }

    function enqueueTasks(items: string[], type: string, predicate: string, parentPredicate: string | undefined): void {
        enqueueNextTask(items, item => item, () => type, predicate, parentPredicate, undefined, () => {
            console.log('  completed');
            moveToNext();
        })
    }


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
        const result: Pair<object, string[]> = waitFor2(consumer => fs.readdir(path, consumer));
        assertEquals(result.first, null);
        const files = result.second.filter(name => !name.startsWith('.'));

        if (isEmpty(files)) {
            fail();
            remove('delete empty directory', 'rmdir', path);
        }
        else {
            enqueueTasks(files, 'fileSystemEntry', 'name', 'directory');
        }
        return true;
    }

    function updateProperty(subject: string, predicate: string, predicatePath: string, value: string, objectValue: string, alreadyUpdated: () => boolean): boolean {
        return getObject(subject, predicate, () => {
            console.log(`  ${predicatePath}: undefined-- > ${value} `);
            update([
                put(subject, predicate, objectValue),
                ...moveToNextStatements(),
            ]);
            return true;
        }, (object: string) => {
            assertEquals(object, objectValue);
            return alreadyUpdated();
        })
    }

    function updateLiteralProperty(subject: string, predicate: string, predicatePath: string, value: string, literalTag: string, alreadyUpdated: () => boolean): boolean {
        return updateProperty(subject, predicate, predicatePath, value, encodeLiteral(literalTag, value), alreadyUpdated);
    }

    function updateLiteralPropertyOnCurrentTask(predicate: string, value: string, literalTag: string, alreadyUpdated: () => boolean): boolean {
        return updateLiteralProperty(currentTask, predicate, predicate, value, literalTag, alreadyUpdated);
    }


    const type = getPropertyFromCurrent('type');

    function processFileSystemPath<T>(path: string, directory: () => T, file: () => T, missing: () => T): T {
        console.log(`processing ${type} ${path}`);
        return stat(path, stat => (stat.isDirectory() ? directory : file)(), missing);
    }

    function enqueueMBTask<T>(mbid: string, resource: string, linkPredicate: string | undefined, enqueued: T, alreadyExists: (id: string) => T): T {
        return enqueueTask(mbid, `mb:${resource}`, 'mb:mbid', undefined, linkPredicate, enqueued, alreadyExists);
    }

    function enqueueMBResourceTask(mbid: string, resource: string, found: (id: string) => boolean): boolean {
        return enqueueMBTask(mbid, resource, undefined, true, found);
    }

    function getStringProperty(name: string) {
        return decodeStringLiteral(getPropertyFromCurrent(name));
    }

    function complyWithRateLimit(server: string, minimumDelay: number) {

        function update(la: number) {
            lastAccessed[server] = la;
        }

        const la = lastAccessed[server];
        if (isUndefined(la)) {
            update(Date.now());
        }
        else {
            const now = Date.now();
            const diff = now - la;
            if (diff <= minimumDelay) {
                setTimeout(getRunner(), diff);
                yieldValue();
            }
            update(now);
        }

    }


    function processMBResource(type: string, inner: Sequence<Predicate<SaxEvent>>, inc: string[], extraAttributes: Matcher<Attributes>, onNoMatch: () => void): void {
        complyWithRateLimit('musicbrainz.org', 1000);
        const mbid = getStringProperty('mb:mbid');
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
        const nextEvent = makeBlockingQueue((push: Consumer<SaxEvent>) => {
            parser.onopentag = tag => push({ type: 'openTag', tag: tag as sax.Tag });
            parser.onerror = fail;
            parser.onclosetag = name => push({ type: 'closeTag', name: name });
            parser.ontext = text => push({ type: "text", text: text });
        });

        let predicates = matchTagAndAttributes('metadata', {
            xmlns: "http://musicbrainz.org/ns/mmd-2.0#"
        }, matchTag(type, matchObject({
            id: expectEquals(mbid),
            ...extraAttributes,
        }), inner));

        for (; ;) {
            if (isUndefined(predicates)) {
                onNoMatch();
                break;
            }
            console.log(asString(predicates.first));
            if (predicates.first(nextEvent())) {
                break;
            }
            predicates = predicates.rest;
        }

    }

    function processMBNamedResource(type: string, subType: Predicate<string>, typeId: Predicate<string>, name: Predicate<string>, sortName: string, additionalTags: Sequence<Predicate<SaxEvent>>, inc: string[], onNoMatch: () => void): void {
        processMBResource(type, concat(
            nameTags(name, sortName),
            additionalTags
        ), inc, {
                type: subType,
                'type-id': typeId
            }, onNoMatch);
    }

    function enqueueArea(mbid: string, found: () => void): void {
        enqueueMBResourceTask(mbid, 'area', () => {
            found();
            return true;
        });
    }



    function url(hostName: string, type: string, id: string): string {
        return `https://${hostName}/${type}/${id}`;
    }

    function wsGet<T>(minimalDelay: number, path: string, params: Dictionary<string>, apiHost: string): T {
        complyWithRateLimit(apiHost, minimalDelay);
        let resourcePath = `/${path}`;
        const paramString = mapDictionary(params, (key, value) => `${key}=${encodeURIComponent(value)}`).join('&');
        if (paramString.length !== 0) {
            resourcePath = `${resourcePath}?${paramString}`
        }
        //console.log(url(hostName, logType, id));
        const run = getRunner();
        https.get({
            hostname: apiHost,
            path: resourcePath,
            port: 443,
            headers: { 'user-agent': 'rasuni-musicscanner/0.0.1 ( https://musicbrainz.org/user/rasuni )' }
        }, run).on("error", fail);
        const resp = yieldValue();
        assertEquals(resp.statusCode, 200);
        const nextChunk = makeBlockingStream((event: string, consumer: Consumer<string>) => resp.on(event, consumer));
        let response = '';
        for (; ;) {
            const chunk = nextChunk();
            if (isUndefined(chunk)) {
                break;
            }
            response += chunk;
        }
        return JSON.parse(response);

    }


    function mbGet<T>(resource: string, params: Dictionary<string>): T {
        //console.log(url('musicbrainz.org', logType, mbid));
        return wsGet(1000, `ws/2/${resource}`, {
            fmt: 'json',
            ...params
        }, 'musicbrainz.org')
    }

    function getMBEntity<T>(type: string, params: Dictionary<string>, idPredicate: string): T {
        const mbid = getStringProperty(idPredicate/*'mb:mbid'*/);
        console.log(url('musicbrainz.org', type, mbid));

        const metaData: Entity = mbGet(`${type}/${mbid}`, params);
        assertEquals(metaData.id, mbid);
        //console.log(metaData.area.id);
        return metaData as any;
    }

    function getMBCoreEntity<T>(type: string): T {
        return getMBEntity(type, {}, 'mb:mbid');
    }

    function enqueueNextEntityTask<T>(items: T[], entity: (item: T) => Entity, type: (item: T) => string, completed: () => boolean): boolean {
        return enqueueNextTask(items, item => entity(item).id, item => `mb:${type(item)}`, 'mb:mbid', undefined, true, completed);
    }


    function enqueueMBEntityId(mbid: string, type: string, alreadyAdded: () => boolean): boolean {
        return enqueueTopLevelTask(mbid, `mb:${type}`, 'mb:mbid', alreadyAdded);
    }

    function enqueueMBEntity(entity: Entity, type: string, alreadyAdded: () => boolean): boolean {
        return enqueueMBEntityId(entity.id, type, alreadyAdded);
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
                () => processDirectory(entryPath),
                () => {
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
                            const common = metaData.common;
                            const acoustid = common.acoustid_id;
                            if (isDefined(acoustid)) {
                                return enqueueTypedTopLevelTask(acoustid, 'acoustid', () => {
                                    const trackId = common.musicbrainz_trackid;
                                    assertDefined(trackId);
                                    return enqueueMBResourceTask(trackId as string, 'track', taskId => {
                                        for (const key of ['title', 'artist']) {
                                            if (getObject(taskId, `mb:${key}`, FALSE, value => {
                                                if ((common as any)[key] === decodeStringLiteral(value)) {
                                                    return false;
                                                }
                                                else {
                                                    fail();
                                                    return true;
                                                }

                                            })) {
                                                return false;
                                            }
                                        }
                                        console.log('  Please check tagging in picard');
                                        moveToNext();
                                        return false;
                                    })
                                });
                            }
                            else {
                                console.error("  acoustid is missing!");
                                return false;
                            };
                        case '.jpg':
                            remove('deleting file', 'unlink', entryPath);
                            return true;
                        default:
                            // *.mkv
                            logError('unknown file type!');
                            return false;
                    }


                }, () => stat(vPath, () => {
                    assertMissing({ predicate: 'directory', object: currentTask });
                    const next = getPropertyFromCurrent('next');
                    const nextStatement = getStream({ subject: currentTask });
                    const updateStatements: UpdateStatement[] = [];
                    for (; ;) {
                        const statement = nextStatement();
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
                    //fail();
                    volumeNotMounted();
                    return false;
                }));
        case 'mb:artist':

            const artist: Artist = getMBCoreEntity('artist');
            return enqueueTypedTopLevelTask(artist.type, 'mb:artist-type', () => enqueueMBEntity(artist.area, 'area', fail));

            return enqueueMBEntity(artist.area, 'area', () => enqueueTypedTopLevelTask(type, 'mb:artist-type', fail));

        case 'mb:area':
            const area: Area = getMBCoreEntity('area');
            //console.log(area);
            return enqueueTypedTopLevelTask(area.type, 'mb:area-type', fail);
        case 'mb:release':
            const release: Release = getMBCoreEntity('release');
            console.log(release);
            return enqueueTypedTopLevelTask(release.status, 'mb:release-status', fail);
        case 'mb:recording':

            const recording: Recording = getMBEntity('recording', {
                inc: 'artists+artist-rels+work-rels'
            }, 'mb:mbid');
            //assert(recording.isrcs.length === 0);
            return enqueueNextEntityTask(recording["artist-credit"], artistCredit => artistCredit.artist, () => 'artist', () => {
                return enqueueNextEntityTask(recording.relations, relation => {
                    const artist = relation.artist;
                    if (isDefined(artist)) {
                        return artist;
                    }
                    else {
                        const work = relation.work;
                        assertDefined(work);
                        return work as Entity;
                    }
                }, relation => {
                    const artist = relation.artist;
                    if (isDefined(artist)) {
                        return 'artist';
                    }
                    else {
                        const work = relation.work;
                        assertDefined(work);
                        return 'work';
                    }
                }, () => {
                    const mbid = recording.id;
                    const releaseList: ReleaseList = mbGet('release', {
                        recording: mbid
                    });
                    const releases = releaseList.releases;
                    assertEquals(releaseList["release-count"], releases.length);
                    return enqueueNextEntityTask(releaseList.releases, release => release, () => 'release', () => {
                        fail();
                        return false;
                    });
                });

            });
        case 'mb:label':
            fail();

            processMBNamedResource('label', expectEquals("Original Production"), expectEquals("7aaa37fe-2def-3476-b359-80245850062d"), expectEquals('Arista'), 'Arista', concat(
                expectPlainTextTag('label-code', '3484'),
                expectUsCountry,


                expectArea('489ce91b-6658-3307-9877-795b68554c98', 'United States', expectIsoList1('US')),
                expectLifeSpan('1974', concat(
                    expectPlainTextTag('end', '2011-10-07'),
                    expectPlainTextTag('ended', 'true'),
                )),
                expectTag('release-list', {
                    count: '2857'
                }, concat(

                    expectRelease('00991de5-3dc9-32c3-830b-baeeec1757af', 'The Monty Python Matching Tie and Handkerchief', expectOfficial, 'US', '489ce91b-6658-3307-9877-795b68554c98', expectCardboard, expectBarcode),
                    expectRelease('0681f65c-302a-462e-9bff-7c25cdcf4188', 'Under the Sun', undefined, 'US', '489ce91b-6658-3307-9877-795b68554c98', undefined, undefined),
                    expectRelease('1f77f98f-a010-4b13-8b11-bdc6df39adc9', "Tryin' to Get the Feeling", expectOfficial, 'US', '489ce91b-6658-3307-9877-795b68554c98', expectCardboard, expectBarcode),
                    expectRelease('25321ea5-4148-4cea-adeb-b04b62dcb8b6', 'Live at Montreux', expectOfficial, 'US', '489ce91b-6658-3307-9877-795b68554c98', expectTextRepresentation, undefined),
                    expectRelease('3957c01f-e4a9-4a98-a17d-386f4bb43657', 'Modern Times', expectOfficial, 'CA', '71bbafaa-e825-3e15-8ca9-017dcad1748b', expectCardboard, expectBarcode)
                ))
            ), ['releases'], () => enqueueArea('489ce91b-6658-3307-9877-795b68554c98', fail))
            //fail();
            return true;
        case 'acoustid':
            const acoustid = getStringProperty('acoustid');
            console.log(url('acoustid.org', 'track', acoustid));
            const result: AcoustIdMetaData = wsGet(334, '/v2/lookup', {
                client: '0mgRxc969N',
                meta: 'recordingids',
                trackid: acoustid
            }, 'api.acoustid.org');
            assertEquals(result.status, "ok");
            const results = result.results;
            assertEquals(results.length, 1);
            const firstResult = results[0];
            assertEquals(firstResult.id, acoustid);
            return enqueueNextEntityTask(firstResult.recordings, recording => recording, () => 'recording', () => {
                console.log("  completed: please check acoustid resource");
                const promise = opn(url('acoustid.org', 'track', acoustid), { wait: false });
                // const run = getRunner();
                promise.then(childProcess => {
                    childProcess.disconnect();
                }, err => {
                    console.log(err);
                    fail();
                })
                //const childProcess = yieldValue();
                //childProcess.disconnect();
                moveToNext();
                return false;
            });
        case 'mb:track':

            const mbid = getStringProperty('mb:mbid');
            console.log(url('musicbrainz.org', 'track', mbid));

            const metaData: ReleaseList = mbGet('release', {
                track: mbid
            });
            //console.log(metaData);
            assertEquals(metaData["release-count"], 1);
            const rels = metaData.releases[0];
            const releaseId = rels.id;

            function findTrack(media: Medium): Track | undefined {
                return media.tracks.find(track => track.id === mbid)
            }

            const medium = rels.media.find(media => isDefined(findTrack(media)));
            const position = (medium as Medium).position;
            return tryAddKeyedTask('mb:medium', {
                "mb:release-id": encodeString(releaseId),
                "mb:position": encodeLiteral('n', `${position}`)
            }, `${releaseId}/${position}`, '  ', undefined, () => {
                moveToNext();
                return true;
            }, () => {
                const track = findTrack(medium as Medium) as Track;
                return enqueueMBEntity(track.recording, 'recording', () => {
                    return updateLiteralPropertyOnCurrentTask('mb:title', track.title, 's', fail);

                    // TODO: update mb:artist on track task

                });
            });


        case 'mb:medium':
            const rel: Release = getMBEntity('release', {
                inc: 'media'
            }, 'mb:release-id');
            assertDefined(rel.media.find(m => m.position == Number(decodeLiteral(getPropertyFromCurrent('mb:position'), 'n'))));
            //console.log(rel);
            return enqueueMBEntityId(getStringProperty('mb:release-id'), 'release', fail);

        default:
            fail();
            return false;
    }

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
    assert(isNull(waitFor(callback => rimraf(dbPath, callback))));
});

function enqueue(type: string, id: string) {
    tryAdd(id, type, type, undefined, '', undefined, () => undefined, () => console.log(`already added ${type} ${id}`));
}


defineCommand("import", "import tasks from music.txt file", [], () => {
    const rl = readline.createInterface({
        input: fs.createReadStream('music.txt'),
        terminal: false
    });
    const nextLine: () => undefined | string = makeBlockingQueue(push => {
        rl.on('line', push);
        rl.on('close', push);
    })
    for (; ;) {
        const nl = nextLine();
        if (isUndefined(nl)) {
            break;
        };
        function handleScheme(scheme: string, noMatch: () => void) {
            const prefix = `${scheme}://`;
            if ((nl as string).startsWith(prefix)) {
                enqueue('url', `${prefix}${(nl as string).substr(prefix.length).split('/').map(decodeURIComponent).map(encodeURIComponent).join('/')}`);
            }
            else {
                noMatch();
            }

        }

        handleScheme('https', () => {
            if (nl.startsWith('/')) {
                enqueue('path', nl);
            }
            else {
                if (nl.startsWith('    ')) {
                    console.log(`ignore file: ${nl.substr(4)}`);
                }
                else {
                    if (nl === '<root>') {
                        console.log('ignore root')
                    }
                    else {
                        handleScheme('http', () => assert(nl === ''));
                    }
                }
            }
        });
    }
});

defineCommand('dump', 'dump database to text format', [], () => {
    const first = getCurrentTask();
    let current = first;

    function getPropertyFromCurrent(name: string): string {
        return getProperty(current, name);
    }

    function getStringProperty(name: string): string {
        return decodeStringLiteral(getPropertyFromCurrent(name));
    }

    function url(domain: string, path: string) {
        console.log(`https://${domain}/${path}`);
    }

    function resource(domain: string, type: string, predicate: string): void {
        url(domain, `${type}/${getStringProperty(predicate)}`);
    }

    function mbResource(type: string): void {
        resource('musicbrainz.org', type, 'mb:mbid');
    }

    function searchType(type: string) {
        url('musicbrainz.org', `search?query=type%3A${getStringProperty(`mb:${type}-type`)}&type=${type}&method=advanced`);
    }

    for (; ;) {


        const type = getPropertyFromCurrent('type');
        switch (type) {
            case 'acoustid':
                resource('acoustid.org', 'track', 'acoustid');
                break;
            case 'fileSystemEntry':
                let path = getStringProperty('name');


                let fso = current;

                function prop(predicate: string): string {
                    return getProperty(fso, predicate);
                }
                function prepend(predicate: string) {
                    return `${decodeStringLiteral(prop(predicate))}/${path}`;
                }

                for (; ;) {
                    fso = prop('directory');
                    if (prop('type') === 'volume') {
                        console.log(prepend('path'));
                        break;
                    }
                    path = prepend('name');
                }
                break;
            case 'volume':
                console.log(getStringProperty('path'));
                break;
            case 'root':
                console.log('<root>');
                break;
            case 'mb:recording':
                mbResource('recording');
                break;
            case 'mb:track':
                mbResource('track');
                break;
            case 'mb:artist':
                mbResource('artist');
                break;
            case 'mb:medium':
                console.log(`https://musicbrainz.org/release/${getStringProperty('mb:release-id')}/disc/${decodeLiteral(getPropertyFromCurrent('mb:position'), 'n')}`);
                break;
            case 'mb:area':
                mbResource('area');
                break;
            case 'mb:release':
                mbResource('release');
                break;
            case 'mb:work':
                mbResource('work');
                break;
            case 'mb:area-type':
                searchType('area');
                break;
            case 'mb:artist-type':
                searchType('artist');
                break;
            default:
                fail();
                //console.log(`https://musicbrainz.org/search?query=type%3A${getStringProperty('mb:area-type')}&type=area&method=advanced`);
                //console.log(`https://musicbrainz.org/release/${getStringProperty('mb:release-id')}/disc/${decodeLiteral(getPropertyFromCurrent('mb:position'), 'n')}`);
                break;
        }
        const next = getPropertyFromCurrent('next');
        if (next === first) {
            break;
        }
        current = next;
    }
});

Fiber(() => {

    commander.parse(process.argv);
    if (!executed) {
        commander.outputHelp();
    }

}).run();
