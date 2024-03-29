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
import * as readline from 'readline';
import opn = require('open');
import * as http from 'http';
import format = require('format-duration');

commander.version('0.0.1').description('music-scanner command line tool');

interface Function<P, R> {
    (parameter: P): R
};

type Consumer<T> = Function<T, void>;

function getRunner<T>(): Consumer<T> {
    const currentFiber = Fiber.current;
    return (value?: T) => currentFiber!.run(value);
}

const yieldValue = Fiber.yield;

type Visitable<T> = Consumer<Consumer<T>>;


function waitFor<R>(asyncFunction: Visitable<R>): R {
    asyncFunction(getRunner());
    return yieldValue();
}

interface Pair<F, S> {
    readonly first: F,
    readonly second: S
}

interface BiFunction<P1, P2, R> {
    (p1: P1, p2: P2): R
};

type BiConsumer<F, S> = BiFunction<F, S, void>;

function waitFor2<F, S>(asyncFunction: Consumer<BiConsumer<F, S>>): Pair<F, S> {
    return waitFor(consumer => asyncFunction((first, second) => consumer({
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

function assertObject(value: any) {
    assertSame(typeof value, 'object');
}

interface Dictionary<T> {
    readonly [name: string]: T;
}

function isUndefined(value: any): value is undefined {
    return value === undefined;
}


function assertEquals(actual: any, expected: any): void {
    const queue: {
        readonly actual: any,
        readonly expected: any;
    }[] = [];
    for (; ;) {
        if (actual !== expected) {
            assertObject(actual);
            assertObject(expected);
            failIf(isNull(expected));

            function push(key: string): void {
                queue.push({
                    actual: actual[key],
                    expected: expected[key]
                });
            }

            const visited = new Set();
            for (const key in actual) {
                push(key);
                visited.add(key);
            }
            for (const key in expected) {
                if (!visited.has(key)) {
                    push(key)
                }
            }
        }
        const next = queue.pop();
        if (isUndefined(next)) {
            break;
        }
        actual = next.actual;
        expected = next.expected;
    }
}

function assertUndefined(value: any): void {
    assert(isUndefined(value));
}


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

type StatementPattern = Partial<Statement<string>>;

type Operation = "put" | "del";

interface UpdateStatement extends Statement<string> {
    readonly operation: Operation
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

function decodeLiteral(literal: string, tag: string): string {
    const segments = literal.split('/');
    assertEquals(segments.length, 2);
    assertSame(segments[0], tag);
    return decodeURIComponent(segments[1]);
}

function decodeStringLiteral(stringLiteral: string): string {
    return decodeLiteral(stringLiteral, 's');
}

interface Provider<T> {
    (): T
};

function isEmpty(array: {
    readonly length: number;
}): boolean {
    return array.length === 0;
}


function makeBlockingQueue<T>(assignProducers: Visitable<T>): Provider<T> {
    const buffer: T[] = [];
    let waiting: boolean = false;
    const run = getRunner();
    assignProducers(item => {
        if (waiting) {
            assertEquals(buffer.length, 0);
            run(item);
        }
        else {
            buffer.push(item);
        }
    });
    return () => {
        if (isEmpty(buffer)) {
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


/////////////

type Registrar<T> = BiConsumer<string, Consumer<T>>;

function makeBlockingStream<T>(on: Registrar<T>): Provider<T> {
    return makeBlockingQueue(push => {

        function register(event: string): void {
            on(event, push);
        }

        register('data');
        register('end');
    });
}

interface Stream<T> {
    readonly on: Registrar<T>;
}

function prepareDBStream<T>(stream: Stream<T>): Provider<T> {

    function on(event: string, handler: Consumer<T>): void {
        stream.on(event, handler);
    }

    on('error', fail);
    return makeBlockingStream(on);
}

function ifDefined<T, D, U>(value: T | undefined, onDefined: Function<T, D>, onUndefined: Provider<U>): D | U {
    return isDefined(value) ? onDefined(value) : onUndefined()
}
function onDefined<T, D,>(value: T | undefined, onDefined: Function<T, D>): D | undefined {
    return ifDefined(value, onDefined, () => undefined)
}

function streamOpt<T, NF, R>(stream: Stream<T>, onEmpty: Provider<NF>, onData: Function<T, R>): NF | R {
    const next = prepareDBStream(stream);
    const data = next();
    return ifDefined(data, dd => {
        assertUndefined(next());
        return onData(dd);
    }, onEmpty);
}

const logError = console.error;

function volumeNotMounted(path: string): void {
    logError(`Volume ${path} not mounted. Please mount!`);
}

const join = path.join;

const dbPath = join(__dirname, 'music-scanner');

let executed = false;

function defineCommand<T>(cmdSyntax: string, description: string, options: string[], action: (...args: T[]) => void) {
    var cmd = commander.command(cmdSyntax).description(description);
    for (const option of options) {
        cmd = cmd.option(option);
    }
    cmd.action((...args: T[]) => {
        action(...args);
        executed = true;
    });
}


const db = levelgraph(level(dbPath), { joinAlgorithm: 'basic' });


function get<T>(pattern: StatementPattern, onEmpty: Provider<T>, onStatement: Function<Statement<string>, T>): T {
    return streamOpt(db.getStream(pattern), onEmpty, onStatement);
}


type StatementEnd = 'subject' | 'object';

function navigate<T>(source: string, predicate: string, isOutDirection: boolean, notFound: Provider<T>, found: Function<string, T>): T {

    const pattern: {
        readonly predicate: string;
        subject?: string;
        object?: string;
    } = {
        predicate: predicate
    }

    function selectField(outDir: StatementEnd, inDir: StatementEnd): StatementEnd {
        return isOutDirection ? outDir : inDir;
    }

    const sourceKey = selectField('subject', 'object');
    pattern[sourceKey] = source;
    return get(pattern, notFound, statement => {

        function verify(key: keyof Statement<string>, expected: string) {
            assertEquals(statement[key], expected);
        }

        verify(sourceKey, source);
        verify('predicate', predicate);

        return found(statement[selectField('object', 'subject')]);
    });
}

function getObject<T>(subject: string, predicate: string, notFound: Provider<T>, found: Function<string, T>): T {
    return navigate(subject, predicate, true, notFound, found);
}


function getProperty(subject: string, name: string): string {
    return getObject(subject, name, fail, obj => obj);
}


function persist(type: Operation, statements: Statement<string>[]): void {
    if (!isEmpty(statements)) {
        assertUndefined(waitFor(callback => db[type](statements, callback)));
    }
}


function mapDictionary<F, T>(source: Dictionary<F>, mapper: BiFunction<string, F, T>): T[] {
    return Object.getOwnPropertyNames(source).map(key => mapper(key, source[key]));
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


const lastAccessed: {
    [name: string]: number;
} = {};


function isDefined<T>(value: T): value is Exclude<T, undefined> {
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
    //readonly toString?: Provider<string>;
}

type Sequence<T> = SequenceNode<T> | undefined;



function getStream(pattern: StatementPattern): Provider<Statement<string>> {
    return prepareDBStream(db.getStream(pattern));
}



const log = console.log;

function getCurrentTask(): string {
    return getObject('root', 'current', () => {
        log('initializing database');

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

function searchOpt<NF, T>(query: Statement<any>[], onEmpty: () => NF, onData: (data: Dictionary<string>) => T): NF | T {
    return streamOpt(db.searchStream(query), onEmpty, onData);
}

function tryAddKeyedTask<T>(type: string, keys: Dictionary<string>, path: Provider<string>, prefix: string, linkPredicate: string | undefined, enqueued: Function<string, T>, alreadyAdded: Function<any, T>): T {

    const currentTask = getCurrentTask();


    function mapAttributeValues<S, T>(mapper: (subject: S, predicate: string, object: string) => T, subject: S): T[] {

        return mapDictionary({
            type: type,
            ...keys
        }, (key, value) => mapper(subject, key, value));
    }

    return searchOpt(mapAttributeValues(statement, db.v('s')), () => {

        // TODO add  is something like this

        log(`${prefix}adding ${type} ${path()}`);
        const taskId = `task/${uuid()}`;
        update([
            ...mapAttributeValues(put, taskId),
            link(taskId, currentTask),
            ...appendToPrev(taskId),
            ...enumOptional(linkPredicate, () => put(currentTask, linkPredicate as string, taskId))
        ]);

        return enqueued(taskId);
    }, alreadyAdded);
}

function encodeLiteral(literalTag: string, rawString: string): string {
    return `${literalTag}/${encodeURIComponent(rawString)}`
}

function encodeString(value: string): string {
    return encodeLiteral('s', value)
}

function encodeNumber(value: number): string {
    return encodeLiteral('n', `${value}`);
}

function tryAdd<T>(key: string | number, type: string, namePredicate: string, parentPredicate: string | undefined, prefix: string, linkPredicate: string | undefined, enqueued: () => T, alreadyAdded: (found: any) => T): T {
    let keys: any = {
    };
    function setName(name: string) {
        keys[namePredicate] = name;
    }
    switch (typeof key) {
        case 'string':
            setName(encodeString(key as string));
            break;
        case 'number':
            setName(encodeNumber(key as number));
            break;
        default:
            fail();
    }
    if (isDefined(parentPredicate)) {
        keys[parentPredicate] = getCurrentTask();
    }
    return tryAddKeyedTask(type, keys, () => `${key}`, prefix, linkPredicate, enqueued, alreadyAdded);
}


function assertDefined(value: any): void {
    failIf(isUndefined(value));
}
interface Entity {
    readonly id: string;
}

interface EntityList {
    readonly count: 0;
}

interface Alias {
    readonly name: string;
}

interface Tag {
    readonly name: string;
}

interface Area {
    readonly id: string;
    readonly type: string;
    readonly name: string;
    readonly 'iso-3166-1-codes'?: string[];
    readonly aliases: Alias[];
    readonly tags: Tag[];
    readonly relations: Relation[];
    //readonly iso1: string;
}

interface Lifespan {
    readonly begin: string;
}

interface Relation {
    readonly artist?: Entity;
    readonly work?: Entity;
    readonly recording?: Entity;
    readonly url?: Entity;
    readonly 'target-type': 'artist' | 'work' | 'recording' | 'url';
}

interface Artist {
    readonly id: string;
    readonly area: Area | null;
    readonly type: string;
    readonly name: string;
    readonly 'life-span': Lifespan;
    readonly relations: Relation[];
    readonly disambiguation?: string;
    readonly "sort-name"?: string;

}

interface ArtistCredit {
    readonly artist: Artist;
    readonly joinphrase: string;
}


interface Recording {
    readonly id: string;
    readonly title: string;
    readonly length: number;
    readonly "artist-credit": ArtistCredit[];
    readonly relations: Relation[];
    readonly isrcs: string[];
}

interface Track {
    readonly id: string;
    readonly recording: Recording;
    readonly title: string;
    readonly "artist-credit": ArtistCredit[];
    readonly position: number;
    readonly length: number;
}

interface Disc {
    readonly id: string;
    readonly releases: Release[];
    readonly offsets: number[];
    readonly sectors: number;
}

interface Medium {
    readonly tracks: Track[];
    readonly position: number;
    readonly format: string;
    readonly discs: Disc[]
}

interface TextRepresentation {
    readonly language: string | null;
    readonly script: string;
}

interface ReleaseEvent {
    readonly area: Entity | null;
}

interface LabelInfo {
    readonly "catalog-number": string | null;
    readonly 'label': Entity | null;
}

interface Release {
    readonly id: string;
    readonly title: string;
    readonly media: Medium[];
    readonly status: string;
    readonly quality: string;
    readonly "text-representation": TextRepresentation;
    readonly date: string;
    readonly "artist-credit": ArtistCredit[];
    readonly "release-events": ReleaseEvent[];
    readonly barcode?: string;
    readonly asin: string;
    readonly "label-info": LabelInfo[];
    readonly 'release-group': Entity;
    readonly country?: string;
    readonly relations: Relation[];
}

interface ReleaseList extends EntityList {
    readonly "release-count": number;
    readonly releases: Release[];
}

interface RecordingList extends EntityList {
    readonly "recording-count": number;
    readonly recordings: Recording[];
}

interface Work {
    readonly id: string;
    readonly title: string;
    readonly type: string;
    readonly relations: Relation[];
    readonly languages: string[];
}

interface WorkList extends EntityList {
    readonly works: Work[];
}


interface AreaList extends EntityList {
    readonly areas: Entity[];
}

interface ArtistList extends EntityList {
    readonly artists: Entity[];
}


interface ReleaseGroupList extends EntityList {
    readonly "release-groups": Entity[];
}

interface AcoustIdRecording {
    readonly id: string;
}

interface AcoustIdTrack {
    readonly id: string;
    readonly recordings: AcoustIdRecording[] | undefined;
}

interface AcoustIdResult {
    readonly status: string;
}

interface AcoustIdMetaData extends AcoustIdResult {
    readonly results: AcoustIdTrack[];
}

interface AcoustIdTracks extends AcoustIdResult {
    readonly tracks: AcoustIdRecording[];
}

function processHandlers(handlers: (() => undefined | boolean)[]): boolean {
    for (const handler of handlers) {
        const res = handler();
        if (isDefined(res)) {
            return res;
        }
    }
    fail();
    return false;
}

type ConformMembers<T, C> = { [K in keyof T]: T[K] extends C ? any : never };

type ConformPropertyName<T, C> = keyof ConformMembers<T, C>;

type LiteralPropertyName<T> = ConformPropertyName<T, string | number | null | undefined>;

interface URL extends Entity {
    readonly resource: string;
}
interface ReleaseGroup extends Entity {
    readonly title: string;
    readonly "artist-credit": ArtistCredit[];
    readonly "releases": Release[];
    readonly relations: Relation[];
}

function findTrack(media: Medium, trackId: string): Track | undefined {
    return media.tracks.find(track => track.id === trackId)
}

function getMediumForTrack(release: Release, trackId: string): Medium {
    const res = release.media.find(media => isDefined(findTrack(media, trackId)));
    assertDefined(res);
    return res as Medium;
}

function getTrack(medium: Medium, trackId: string): Track {
    const res = findTrack(medium, trackId);
    assertDefined(res);
    return res as Track;
}

function url(hostName: string, type: string, id: string): string {
    return `https://${hostName}/${type}/${id}`;
}

function browser(url: string) {
    opn(url, { wait: false });
}
function openBrowser(server: string, type: string, id: string): void {
    browser(url(server, type, id));
}

function escapeLucene(key: string | number): string {
    //  console.log(`${key}`.replace(/[!"()&+:]|\||\{|\}|\[|\]|\^|\~|\*|\?|\\|\/|\-/g, s => `\\${s}`));
    return `${key}`.replace(/[!"()&+:]|\||\{|\}|\[|\]|\^|\~|\*|\?|\\|\/|\-/g, s => `\\${s}`);
}

function mb(name: string): string {
    failIf(name.startsWith('mb:'));
    return `mb:${name}`;
}


function div(dividend: number, divisor: number): number {
    return Math.round(dividend / divisor);
}

function openMB(type: string, id: string): void {
    openBrowser('musicbrainz.org', type, id);
}


function withVolumeAndEntryPath<T>(fileSystemEntry: string, joinEntryPath: (name: string, path: string) => string, process: (volume: string, entryPath: string) => T): T {

    let currentEntry = fileSystemEntry;

    function property(name: string): string {
        return getProperty(currentEntry, name);
    }

    function stringProperty(name: string): string {
        return decodeStringLiteral(property(name))
    }

    function name(): string {
        return stringProperty('name');
    }

    let ePath = name();


    for (; ;) {
        currentEntry = property('directory');
        const type = property('type');
        if (type === 'volume') {
            break;
        }
        assertEquals(type, 'fileSystemEntry');
        ePath = joinEntryPath(name(), ePath);
    }

    return process(stringProperty('path'), ePath);

}

function withJoinedVolumeAndEntryPath<T>(entryId: string, process: (volume: string, entryPath: string) => T): T {
    return withVolumeAndEntryPath(entryId, join, process)
}


function withFileSystemObjectPath<T>(entryId: string, path: (entryPath: string, volume: string) => T): T {
    return withJoinedVolumeAndEntryPath(entryId, (vPath, entryPath) => path(join(vPath, entryPath), vPath))

}

function getPath(entryId: string): string {
    return withJoinedVolumeAndEntryPath(entryId, join);
}


function stat<T>(path: string, success: (stats: fs.Stats) => T, missing: () => T): T {
    const result: Pair<NodeJS.ErrnoException | null, fs.Stats> = waitFor2((consumer: (first: NodeJS.ErrnoException | null, second: fs.Stats) => void) => fs.stat(path, consumer));
    const err = result.first;
    if (isNull(err)) {
        return success(result.second);
    }
    else {
        assertSame(err.code, 'ENOENT');
        return missing();
    }
}

function getAudioMetaData(filePath: string): mm.IAudioMetadata {
    const promise: Promise<mm.IAudioMetadata> = mm.parseFile(filePath);
    const fiber = Fiber.current;
    promise.then((value: mm.IAudioMetadata) => fiber!.run({ type: "metadata", metaData: value }), (err: any) => fiber!.run({ type: "error", error: err }));
    const r = yieldValue();

    assertEquals(r.type, 'metadata');
    //let metaData: mm.IAudioMetadata = r.metaData;
    return r.metaData;
}

function excludeNull<T>(data: T | null, predicate: (data: T) => boolean): boolean {
    return data !== null && predicate(data);
}

function searchSubject<NF, R>(attributes: Dictionary<string>, additionalFilter: Statement<any>[], notFoundResult: NF, found: (result: any) => R): NF | R {

    return searchOpt([
        ...mapDictionary(attributes, (predicate, object) => statement(db.v('entity'), predicate, object)),
        ...additionalFilter
    ], () => notFoundResult, found);

}
function searchOptMBEntity<R>(type: string, mbid: string, additionalFilter: Statement<any>[], found: (result: any) => R): R | undefined {

    return searchSubject({
        type: mb(type),
        'mb:mbid': encodeString(mbid)
    }, additionalFilter, undefined, found);
}

function failIfSame(value1: any, value2: any): void {
    failIf(value1 === value2);
}

function failIfZero(value: number): void {
    failIfSame(value, 0);
}

interface LengthHolder {
    readonly length: number;
}

function failIfEmpty(lengthHolder: LengthHolder): void {
    failIfZero(lengthHolder.length)
}

function assertMissing(pattern: StatementPattern): void {
    get(pattern, () => { }, fail);
}

function setCurrent(newCurrent: string): UpdateStatement[] {
    return updateObjectFromCurrent('root', 'current', newCurrent);
}

function removeCurrent(): void {
    const currentTask = getCurrentTask();
    const next = getProperty(currentTask, 'next');
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
    update([
        ...appendToPrev(next),
        ...setCurrent(next),
        ...updateStatements
    ]);
    assertMissing({ subject: currentTask });
}

function getPropertyFromCurrent(name: string): string {
    return getProperty(getCurrentTask(), name);
}

function moveToNextStatements(): UpdateStatement[] {
    return setCurrent(getPropertyFromCurrent('next'));
}

function moveToNext(): void {
    update(moveToNextStatements());
}

function enqueueTask<T>(key: string | number, type: string, namePredicate: string, parentPredicate: string | undefined, linkPredicate: string | undefined, enqueued: T, alreadyAdded: (id: string) => T): T {
    return tryAdd(key, type, namePredicate, parentPredicate, '  ', linkPredicate, () => {
        moveToNext();
        return enqueued;
    }, (found: any) => alreadyAdded(found.s))
}


function enqueueUnlinkedTask<T>(key: string | number, type: string, namePredicate: string, parentPredicate: string | undefined, alreadyAdded: (id: string) => T): boolean | T {
    return enqueueTask<boolean | T>(key, type, namePredicate, parentPredicate, undefined, true, alreadyAdded);
}

function enqueueTopLevelTask<T>(key: string | number, type: string, namePredicate: string, alreadyAdded: (id: string) => T): boolean | T {
    return enqueueUnlinkedTask(key, type, namePredicate, undefined, alreadyAdded);
}

function enqueueTypedTopLevelTask(key: string | number, type: string, alreadyAdded: () => void): boolean | undefined {
    return enqueueTopLevelTask(key, type, type, () => {
        alreadyAdded();
        return undefined
    });
}

function processCurrent(): boolean {
    const currentTask = getCurrentTask();


    function openTrack(mbid: string): void {
        openMB('track', mbid);
        moveToNext();
    }


    function fsOp(removeMethod: 'unlink' | 'rmdir', path: string) {
        assertSame(waitFor(cb => fs[removeMethod](path, cb)), null);
    }

    function remove(message: string, removeMethod: 'unlink' | 'rmdir', path: string): void {
        log(`  ${message}`);
        fsOp(removeMethod, path);
        //assertSame(waitFor(cb => fs[removeMethod](path, cb)), null);
        moveToNext();
    }

    function enqueueNextTask<T, R>(items: T[], processTypedItem: (item: T, handler: (name: string, type: string) => boolean) => boolean, predicate: string, parentPredicate: string | undefined, foundResult: R, completed: () => R): R {
        for (const item of items) {
            //            console.log(item);
            if (processTypedItem(item, (name: string, type: string) => enqueueUnlinkedTask(name, type, predicate, parentPredicate, () => false))) {
                return foundResult;
            }
        }
        return completed();
    }


    function completed(): void {
        log('  completed');
        moveToNext();
    }

    function enqueueNextTypedTask<T, R>(items: T[], data: (item: T) => string | null, type: string, predicate: string, parentPredicate: string | undefined, foundResult: R, completed: () => R): R {
        return enqueueNextTask(items, (item, handler) => {
            const d = data(item);
            assertDefined(d);
            return excludeNull(d, dnn => handler(dnn, type))
        }, predicate, parentPredicate, foundResult, () => completed())
    }

    function enqueueNextItemTask<T>(items: string[], type: string, predicate: string, parentPredicate: string | undefined, foundResult: T, completed: () => T): T {
        return enqueueNextTypedTask<string, T>(items, item => item, type, predicate, parentPredicate, foundResult, () => completed())
    }

    function enqueueTasks(items: string[], type: string, predicate: string, parentPredicate: string | undefined): void {
        enqueueNextItemTask(items, type, predicate, parentPredicate, undefined, completed)
    }



    function processDirectory(path: string): boolean {
        const result: Pair<object | null, string[]> = waitFor2((consumer: BiConsumer<object | null, string[]>) => fs.readdir(path, consumer));
        assertEquals(result.first, null);
        const allFiles = result.second;
        const files = allFiles.filter(name => !name.startsWith('.'));

        if (isEmpty(files)) {
            //fail();
            for (const hiddenFile of allFiles) {
                fsOp('unlink', join(path, hiddenFile));
            }
            remove('delete empty directory', 'rmdir', path);
        }
        else {
            enqueueTasks(files, 'fileSystemEntry', 'name', 'directory');
        }
        return true;
    }


    //const type = getPropertyFromCurrent('type');

    function processFileSystemPath<T>(path: string, directory: () => T, file: () => T, missing: () => T): T {
        log(path);
        return stat(path, stat => (stat.isDirectory() ? directory : file)(), missing);
    }


    function enqueueMBTask<T>(mbid: string, resource: string, linkPredicate: string | undefined, enqueued: T, alreadyExists: (id: string) => T): T {
        return enqueueTask(mbid, mb(resource), 'mb:mbid', undefined, linkPredicate, enqueued, alreadyExists);
    }

    function enqueueMBResourceTask<T>(mbid: string, resource: string, found: (id: string) => T): boolean | T {
        return enqueueMBTask<boolean | T>(mbid, resource, undefined, true, found);
    }

    function getStringProperty(name: string): string {
        return decodeStringLiteral(getPropertyFromCurrent(name));
    }

    function httpsGet(apiHost: string, minimumDelay: number, resourcePath: string): http.IncomingMessage {

        function update(la: number) {
            lastAccessed[apiHost] = la;
        }

        const la = lastAccessed[apiHost];
        ifDefined(la, lad => {
            const now = Date.now();
            const diff = now - lad;
            if (diff <= minimumDelay) {
                setTimeout(getRunner(), diff);
                yieldValue();
            }
            update(now);
        }, () => update(Date.now()));

        ifDefined(la, lad => {
            const now = Date.now();
            const diff = now - lad;
            if (diff <= minimumDelay) {
                setTimeout(getRunner(), diff);
                yieldValue();
            }
            update(now);
        }, () => update(Date.now()));
        /*
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
        */
        const run = getRunner();
        https.get({
            hostname: apiHost,
            path: resourcePath,
            port: 443,
            headers: { 'user-agent': 'rasuni-musicscanner/0.0.1 ( https://musicbrainz.org/user/rasuni )' }
        }, run).on("error", fail);
        return yieldValue();
    }

    function wsGet<T, R>(minimalDelay: number, path: string, params: Dictionary<string>, apiHost: string, found: (data: T) => R, notFound: () => R, moved: () => R): R {
        let resourcePath = `/${path}`;
        const paramString = mapDictionary(params, (key, value) => `${key}=${encodeURIComponent(value)}`).join('&');
        if (paramString.length !== 0) {
            resourcePath = `${resourcePath}?${paramString}`
        }
        let retryCount = 0;
        for (; ;) {
            //console.log(resourcePath);
            const resp = httpsGet(apiHost, minimalDelay, resourcePath);
            switch (resp.statusCode) {
                case 200:
                    const nextChunk = makeBlockingStream((event: string, consumer: Consumer<string>) => resp.on(event, consumer));
                    let response = '';
                    for (; ;) {
                        const chunk = nextChunk();
                        if (isUndefined(chunk)) {
                            break;
                        }
                        response += chunk;
                    }
                    return found(JSON.parse(response));
                case 404:
                    return notFound();
                case 503:
                    if (retryCount === 5) {
                        return fail();
                    }
                    retryCount++;
                    break;
                case 301:
                    return moved();
                default:
                    return fail();
            }
        }
    }


    function mbGet<T, R>(resource: string, params: Dictionary<string>, found: (data: T) => R, notFound: () => R, moved: () => R): R {
        //console.log(url('musicbrainz.org', logType, mbid));
        return wsGet(1000, `ws/2/${resource}`, {
            fmt: 'json',
            ...params
        }, 'musicbrainz.org', found, notFound, moved);
    }

    function mbGetList<T>(resource: string, params: Dictionary<string>, offset: number): T {
        return mbGet<T, T>(resource, {
            ...params,
            limit: "100",
            offset: `${offset}`
        }, data => data, fail, fail);
    }



    function fetchReleaseForTrack<T>(trackId: string, params: Dictionary<string>, found: (release: Release) => T, notFound: () => T) {
        return mbGet<ReleaseList, T>('release', {
            track: trackId,
            limit: '2',
            ...params
        }, metaData => {
            assertEquals(metaData["release-count"], 1);
            return found(metaData.releases[0]);
        }, notFound, fail);
    }

    function getReleaseForTrack<T>(trackId: string, additionalIncs: string, found: (release: Release) => T, notFound: () => T): T {
        return fetchReleaseForTrack(trackId, {
            inc: `artist-credits${additionalIncs}`
        }, found, notFound);
        /*
        return mbGet<ReleaseList, T>('release', {
            track: trackId,
            inc: `artist-credits${additionalIncs}`,
            limit: '1'
        }, metaData => {
            assertEquals(metaData["release-count"], 1);
            return found(metaData.releases[0]);
        }, notFound, fail);
        */
        /*
        assertEquals(metaData["release-count"], 1);
        return metaData.releases[0];
        */
    }

    function deleteCurrent(reason: string): () => true {
        return () => {
            deleteCurrentTask(reason);
            return true;
        }
    }

    function getTypedMBEntity<T extends Entity>(idPredicate: string, type: string, resourceType: string, params: Dictionary<string>, path: (mbid: string) => string, found: (data: T) => boolean): boolean {
        const mbid = getStringProperty(idPredicate);
        log(url('musicbrainz.org', type, path(mbid)));
        // console.log(params);
        return mbGet(`${resourceType}/${mbid}`, params, (metaData: Entity) => {
            const newId = metaData.id;
            assertSame(newId, mbid);
            if (newId === mbid) {
                return found(metaData as T);
            }
            else {
                deleteCurrentTask(`merged into ${newId}`);
                return true;
            }
        }, deleteCurrent('not found'), deleteCurrent('moved (merged)'));
    }

    function getMBEntity<T extends Entity>(type: string, params: Dictionary<string>, idPredicate: string, path: (mbid: string) => string, found: (data: T) => boolean): boolean {
        return getTypedMBEntity(idPredicate, type, type, params, path, found);
    }


    function getMBCoreEntity<T extends Entity>(type: string, incs: string[], found: (data: T) => boolean): boolean {
        return getMBEntity(type, { inc: `artist-rels+recording-rels+work-rels+url-rels+series-rels${incs.map(inc => `+${inc}`).join('')}` }, 'mb:mbid', path => path, found);
    }

    function enqueueNextEntityTask<T, R>(items: T[], entity: (item: T) => Entity | null, type: (item: T) => string, completed: () => R): boolean | R {
        return enqueueNextTask<T, boolean | R>(items, (item, handler) => excludeNull(entity(item), enn => handler(enn.id, mb(type(item)))), 'mb:mbid', undefined, true, completed);
    }


    function enqueueMBEntityId<T>(mbid: string, type: string, alreadyAdded: () => T): boolean | T {
        return enqueueTopLevelTask(mbid, mb(type), 'mb:mbid', alreadyAdded);
    }

    function enqueueMBEntity<T>(source: T, property: ConformPropertyName<T, Entity | null>): () => boolean | undefined {
        const entity: Entity | null = source[property] as any;
        return () => isNull(entity) ? undefined : enqueueMBEntityId(entity.id, property as string, () => undefined);
    }


    function acoustIdGet<T extends AcoustIdResult>(path: string, params: Dictionary<string>): T {
        const result: AcoustIdResult = wsGet<T, T>(334, `v2/${path}`, {
            client: '0mgRxc969N',
            ...params
        }, 'api.acoustid.org', data => data, fail, fail);
        assertEquals(result.status, "ok");
        return result as T;
    }

    function tryEnqueueTypedTopLevelTask(key: string | number, type: string): boolean | undefined {
        return enqueueTypedTopLevelTask(key, type, () => { });
    }

    function processAttribute<T>(record: T, attribute: LiteralPropertyName<T>, type: string): undefined | boolean {
        const value = record[attribute] as any as string | number | null | undefined;
        return isNull(value) || isUndefined(value) ? undefined : tryEnqueueTypedTopLevelTask(value, mb(`${type}-${attribute}`));
        /*
        if (isNull(value) ) {
            return undefined;
        }
        else {
            return ifDefined(value, dvalue => enqueueTypedTopLevelTask(dvalue, mb(`${type}-${attribute}`))
        }
        return isNull(value) || ifDefined(value, dvalue => enqueueTypedTopLevelTask(dvalue, mb(`${type}-${attribute}`)), () => undefined);
        */
    }

    function attributeHandler<T>(record: T, attribute: LiteralPropertyName<T>, type: string): () => undefined | boolean {
        return () => processAttribute(record, attribute, type);
    }

    function enqueueNextEntityFromList<T>(entities: Entity[], entityType: string, completed: () => T): boolean | T {
        return enqueueNextEntityTask(entities, entity => entity, () => entityType, completed);
    }

    /*
    function searchOptMBEntity<R>(type: string, mbid: string, additionalFilter: Statement<any>[], notFound: () => R, found: (result: any) => R) {
        function pattern(predicate: string, object: string): Statement<any> {
            return statement(db.v('entity'), predicate, object);
        }
        return searchOpt([
            pattern('type', mb(type)),
            pattern('mb:mbid', encodeString(mbid)),
            ...additionalFilter
        ], notFound, found);
    }
    */
    /*
 
     function searchMBEntityTask<R>(type: string, mbid: string, notFound: () => R, found: (taskId: string) => R) {
         return searchOptMBEntity(type, mbid, [], notFound, result => found(result.entity));
     }
 */

    /*
    function withPlaylistForEntity<T>(type: string, entity: Entity, missing: () => T, existing: (playlistTask: string) => T): T {
        return searchMBEntityTask(type, entity.id, fail, taskId => getObject(taskId, 'playlist', missing, existing));
    }
    */

    function processSearch<LT extends EntityList, ET extends Entity>(type: string, fieldName: string, literalType: string, searchField: string, taskType: string, entities: ConformPropertyName<LT, ET[]>, match: (entity: ET, searchValue: string) => boolean): boolean {
        const searchValue = decodeLiteral(getPropertyFromCurrent(mb(`${taskType}-${fieldName}`)), literalType);
        const escapedKey = `"${escapeLucene(searchValue)}"`;
        const url = `https://musicbrainz.org/search?query=${searchField}%3A${encodeURIComponent(escapedKey)}&type=${type}&method=advanced`;
        log(url);
        //const escapedKey = escapeLucene(key);
        if (isEmpty(escapedKey)) {
            deleteCurrentTask('empty search key');
            return true;
        }
        else {
            let empty: boolean = true;
            let offset = 0;
            for (; ;) {
                const list: LT = mbGetList(type, { query: `${searchField}:${escapedKey}` }, offset);
                const entityValues: ET[] = list[entities] as any;
                for (const item of entityValues) {
                    if (match(item, searchValue) /*item[field] === compareValue(searchValue)*/) {
                        empty = false;
                        if (enqueueUnlinkedTask(item.id, mb(type), 'mb:mbid', undefined, () => false)) {
                            return true;
                        }

                    }
                }
                offset += entityValues.length;
                if (list.count <= offset) {
                    break;
                }
            }
            //assertSame(list.count, offset);
            // do not know why this is there
            //entities.forEach(entity => withPlaylistForEntity(type, entity, () => undefined, fail));
            if (empty) {
                deleteCurrentTask('no matching search results');
                return true;
            }
            else {
                log('  complete: all search results enqueued');
                browser(url);
                moveToNext();
                return false;
            }



            //                    return enqueueNextTask();
            /*
            return enqueueNextEntityFromList(filtered, type, () => {
                assertSame(list.count, entities.length);
                // do not know why this is there
                //entities.forEach(entity => withPlaylistForEntity(type, entity, () => undefined, fail));
                //fail();
                log('  complete: all search results enqueued');
                browser(url);
                moveToNext();
                return false;
            });
            */
            //              }
            //}
            /*
 
            switch (type) {
                case 'area':
                    return process((list as AreaList).areas);
                case 'artist':
                    return process((list as ArtistList).artists);
                case 'release':
                    return process((list as ReleaseList).releases);
                case 'recording':
                    // No differentiation between
                    // https://musicbrainz.org/search?query=recording%3A%22Anything%20%5C(2B3%20instrumental%20mix%5C)%22&type=recording&method=advanced
                    // Anything (2B3 Instrumental Mix) and
                    // Anything (2B3 instrumental mix)
                    // need to filter recordings according title
 
                    return process((list as RecordingList).recordings);
                case 'work':
                    return process((list as WorkList).works);
                case 'release-group':
                    return process((list as ReleaseGroupList)["release-groups"]);
                default:
                    return fail();
            }
            */
            //}
        }

        /*
                return handleSearch(`search?query=${searchField}%3A${encodeURIComponent(escapedKey)}&type=${type}&method=advanced`, escapedKey, type, searchField, entities => {
                    entities.forEach(entity => withPlaylistForEntity(type, entity, () => undefined, fail));
                    //fail();
                    log('  complete: all search results enqueued');
                    moveToNext();
                    return true;
        
                })
                
                //        failIf(isEmpty(escapedKey)); // found issue with empty barcode
                // const searchField = isDefined(queryField) ? queryField : field;
                log(`https://musicbrainz.org/search?query=${searchField}%3A${encodeURIComponent(escapedKey)}&type=${type}&method=advanced`);
                if (isEmpty(escapedKey)) {
                    deleteCurrentTask('empty search key');
                    return true;
                }
                else {
                    const list: EntityList = mbGetList(type, { query: `${searchField}:${escapedKey}` });
                    const count: number = list.count;
                    if (count === 0) {
                        deleteCurrentTask('no search results');
                        return true;
                    }
                    else {
                        function process(entities: Entity[]): boolean {
                            return enqueueNextEntityFromList(entities, type, () => {
                                assert(count === entities.length);
                                //entities.forEach(entity => searchMBEntityTask(type, entity.id, fail, taskId => getObject(taskId, 'playlist', () => undefined, fail)));
                                entities.forEach(entity => withPlaylistForEntity(type, entity, () => undefined, fail));
                                //fail();
                                log('  complete: all search results enqueued');
                                moveToNext();
                                return true;
                            });
                        }
                        switch (type) {
                            case 'area':
                                return process((list as AreaList).areas);
                            case 'artist':
                                return process((list as ArtistList).artists);
                            case 'release':
                                return process((list as ReleaseList).releases);
                            case 'recording':
                                return process((list as RecordingList).recordings);
                            case 'work':
                                return process((list as WorkList).works);
                            default:
                                return fail();
                        }
                    }
                }
                */

    }


    function processStringSearch<LT extends EntityList, ET extends Entity>(type: string, fieldName: string, queryField: string, taskType: string, list: ConformPropertyName<LT, ET[]>, matcher: (entity: ET, searchValue: string) => boolean): boolean {
        return processSearch(type, fieldName, 's', queryField, taskType, list, matcher);
    }

    function deleteCurrentTask(msg: string): void {
        log(`  ${msg} -> remove entry`);
        removeCurrent();
    }

    function processList<T>(items: T[] | undefined, entity: (item: T) => Entity | null, type: (item: T) => string): () => undefined | boolean {
        return () => onDefined(items, ditems => enqueueNextEntityTask(ditems, entity, type, () => undefined));
    }


    function getNumberProperty(name: string): number {
        return Number(decodeLiteral(getPropertyFromCurrent(name), 'n'));
    }

    function processRelations(relations: Relation[]): () => boolean | undefined {
        return processList(relations, relation => relation[relation["target-type"]] as Entity, relation => relation["target-type"]);
    }

    function getReferencesToCurrent(pred: string): Provider<Statement<string>> {
        return getStream({ predicate: pred, object: currentTask })
    }

    function processListAttribute<T, E>(source: T, collection: ConformPropertyName<T, E[]>, entity: (elem: E) => Entity | null, elementName: string): () => boolean | undefined {
        return processList(source[collection] as any as E[], elem => entity(elem), () => elementName);
    }

    function processEntityList<T, E>(source: T, collection: ConformPropertyName<T, E[]>, elementName: ConformPropertyName<E, Entity | null>): () => boolean | undefined {
        return processListAttribute(source, collection, elem => (elem as any)[elementName], elementName as string);
    }

    function handleAttributes<T, M>(entity: T, collection: ConformPropertyName<T, M[]>, field: ConformPropertyName<M, string | null>, entityType: string, subType: string): () => boolean | undefined {
        const type = mb(`${entityType}-${subType}`);
        return () => enqueueNextTypedTask<M, boolean | undefined>(entity[collection] as any as M[], member => member[field] as any, type, type, undefined, true, () => undefined);
    }


    function tryAddKeyedTaskIndent<T>(type: string, keys: Dictionary<string>, name: () => string, enqueued: (taskId: string) => T, alreadyExisting: (taskId: string) => T): T {
        return tryAddKeyedTask(type, keys, name, '  ', undefined, enqueued, alreadyExisting)
    }


    function updateObject<T>(predicate: string, object: string | undefined, alreadyUpToDateRes: () => T): true | T {

        function checkUpdateRequired(alreadyUpToDate: boolean, from: string, extend: (statements: UpdateStatement[]) => UpdateStatement[]): true | T {
            if (alreadyUpToDate) {
                return alreadyUpToDateRes();
            }
            else {
                log(`  ${predicate}: ${from} --> ${object}`);
                update(extend([
                    put(currentTask, predicate, object as string),
                    ...moveToNextStatements()
                ]));
                return true;
            }
        }


        return getObject<true | T>(currentTask, predicate, () =>
            // not found
            checkUpdateRequired(isUndefined(object), 'undefined', stms => stms), (existingObject: string) => {
                assertDefined(object);
                return checkUpdateRequired(object === existingObject, existingObject, statements => [
                    del(currentTask, predicate, existingObject),
                    ...statements
                ]);
                /*
                if (object === existingObject) {
                    return undefined;
                }
                else {
                    assertDefined(object);
                    fail();
                    log(`  ${predicate}: ${existingObject} --> ${object}`);
                    update([
                        del(currentTask, predicate, existingObject),
                        put(currentTask, predicate, object as string),
                        ...moveToNextStatements(),
                    ]);
                    return true;
                }
                */
                /*
                assertSame(object, existingObject);
                return undefined;
                */
            })
    }

    function openRecording(path: string): void {
        openMB('recording', path);

    }

    function processNextEntityFromList(entities: Entity[], type: string) {
        return enqueueNextEntityFromList(entities, type, () => undefined)
    }


    const COMPLETED_HANDLER = () => {
        completed();
        return true;
    }

    function processEntitySearch<LT extends EntityList, ET extends Entity>(type: string, fieldName: string, queryField: string, list: ConformPropertyName<LT, ET[]>, matcher: (entity: ET, searchValue: string) => boolean): boolean {
        return processStringSearch(type, fieldName, queryField, type, list, matcher);
    }

    function processDefaultSearch<LT extends EntityList, ET extends Entity>(type: string, fieldName: string, list: ConformPropertyName<LT, ET[]>, matcher: BiFunction<ET, string, boolean>): boolean {
        return processEntitySearch(type, fieldName, fieldName, list, matcher);
    }


    function handleNotMountedVolume(vPath: string, mounted: () => void): boolean {
        return stat(vPath, () => {
            mounted();
            return true;

        }, () => {
            //logError(`Volume ${vPath} not mounted. Please mount!`);
            //fail();
            volumeNotMounted(vPath);
            return false;
            //return fail();
        });
    }


    function processMBCoreEntityInc<T extends Entity>(type: string, incs: string[], handlers: ((entity: T, type: string) => undefined | boolean)[]): boolean {
        return getMBCoreEntity<T>(type, incs, entity => processHandlers(handlers.map(handler => (() => handler(entity, type)))));
    }

    function processMBCoreEntity<T extends Entity>(type: string, handlers: ((entity: T, type: string) => undefined | boolean)[]): boolean {
        return processMBCoreEntityInc<T>(type, [], handlers);
    }

    function processAttributeHandler<T>(field: LiteralPropertyName<T>): (record: T, type: string) => undefined | boolean {
        return (record: T, type: string) => processAttribute<T>(record, field, type);
    }

    function enqueueMedium<T>(releaseId: string, position: number, notFoundResult: T): T | true {
        return tryAddKeyedTaskIndent<T | true>('mb:medium', {
            "mb:release-id": encodeString(releaseId),
            "mb:position": encodeNumber(position)
        }, () => `${releaseId}/${position}`, () => {
            moveToNext();
            return true;
        }, () => notFoundResult);
    }


    function processEntityTypeSearch<LT extends EntityList, ET extends Entity>(type: string, fieldName: string, list: ConformPropertyName<LT, ET[]>, matcher: (entity: ET, searchValue: string) => boolean): boolean {
        return processEntitySearch(type, fieldName, type, list, matcher);
    }


    function notInCollection<T>(alreadyUpToDate: () => T): true | T {
        return updateObject('in-collection', 'false', alreadyUpToDate);
    };

    interface ArtistCreditsHolder {
        readonly "artist-credit": ArtistCredit[];
    };

    function processArtistCredits(creditHolder: ArtistCreditsHolder): () => boolean | undefined {
        return processEntityList<ArtistCreditsHolder, ArtistCredit>(creditHolder, 'artist-credit', 'artist');
    }

    function browse(type: string, linkedType: string, entity: Entity): boolean | undefined {
        let filter: any = {};
        filter[linkedType] = entity.id;
        const list: any = mbGetList(type, filter, 0);
        const items = list[`${type}s`];
        //assertEquals(releaseList["release-count"], releases.length);
        return enqueueNextEntityFromList(items, type, () => {
            assertEquals(list[`${type}-count`], items.length);
            return undefined;
        });
    }

    function enqueueNextIdTask<T, M>(obj: T, arrayProp: ConformPropertyName<T, M[]>, id: (member: M) => string, type: string): boolean | undefined {
        return enqueueNextTask(obj[arrayProp] as any as M[], (member: M, handler: (name: string, type: string) => boolean) => handler(id(member), type), type, undefined, true, () => undefined);
    }

    const type = getPropertyFromCurrent('type');
    switch (type) {
        case 'root':
            log('processing root');
            enqueueTasks(['/Volumes/Musik', '/Volumes/music', '/Volumes/Qmultimedia', '/Users/ralph.sigrist/Music/iTunes/ITunes Media/Music'], 'volume', 'path', undefined /*() => { }*/);
            return true;
        case 'volume':
            const volumePath = getStringProperty('path'); // getStringProperty('path');
            return processFileSystemPath(volumePath, () => processDirectory(volumePath), fail, () => {
                volumeNotMounted(volumePath);
                return false;
            });
        case 'fileSystemEntry':
            return withFileSystemObjectPath(currentTask, (entryPath, vPath) => processFileSystemPath(entryPath,
                () => processDirectory(entryPath),
                () => {
                    switch (path.extname(entryPath)) {
                        case '.flac':
                        case '.m4a':
                        case '.mp4':
                        case '.wma':
                            /*
 
                            const promise: Promise<mm.IAudioMetadata> = mm.parseFile(entryPath);
                            const fiber = Fiber.current;
                            promise.then((value: mm.IAudioMetadata) => fiber.run({ type: "metadata", metaData: value }), (err: any) => fiber.run({ type: "error", error: err }));
                            const r = yieldValue();
 
                            assertEquals(r.type, 'metadata');
                            */
                            const metaData: mm.IAudioMetadata = getAudioMetaData(entryPath);
                            const common = metaData.common;
                            const acoustid = common.acoustid_id;
                            const trackId = common.musicbrainz_trackid;
                            //const mbid = trackId as string;
                            let recordingIdResult: string | undefined = undefined;
                            const recordingId = () => {
                                if (isUndefined(recordingIdResult)) {
                                    const mbid = trackId as string;
                                    const release = getReleaseForTrack(mbid, '', release => release, fail);
                                    const medium = getMediumForTrack(release, mbid);
                                    const track = getTrack(medium, mbid);
                                    recordingIdResult = track.recording.id;
                                }
                                return recordingIdResult;
                            }

                            function checkId(id: string | undefined, enqueue: (id: string) => boolean | undefined, idName: string): () => boolean | undefined {
                                return () => ifDefined(id, enqueue, () => {
                                    logError(`  ${idName} is missing!`);
                                    return false;
                                });

                            }

                            /*
 
                            function searchMBEntityTask1<R>(type: string, mbid: string, notFound: () => R, found: (taskId: string) => R) {
                                return searchOptMBEntity(type, mbid, [], notFound, result => found(result.entity));
                            }
                            */
                            return processHandlers([

                                // verify tag for acoustid does exit
                                checkId(acoustid,
                                    // if tag exist, try to enque acoustid trask 
                                    id => tryEnqueueTypedTopLevelTask(id, 'acoustid'),
                                    'acoustid'
                                ),

                                // verify tag for track-id does exit
                                checkId(trackId,
                                    // tag exists
                                    // check if track-id exists in mb database
                                    // must check whether or not tack id is still valid, getReleaseForTrack must be successfull, if not then re-tagging is required.
                                    // otherwhise newly added track will be removed as next, and readded --> in a loop
                                    tid => fetchReleaseForTrack(tid, {},
                                        // track-id is valid
                                        // now try to enqueue track task
                                        () => enqueueMBResourceTask(tid, 'track',
                                            // already enqueued, set track
                                            () => updateObject('track', tid, () => undefined)
                                        ),
                                        // invalid track id
                                        () => {
                                            logError('  Track-id is invalid. Please re-tag file!');
                                            return false;
                                        }
                                    ),
                                    'trackid'
                                ),

                                () => {
                                    return fail();  // track recordinf shall be updated in addition to track
                                },
                                () => {
                                    const acoustidForRecording: AcoustIdTracks = acoustIdGet('track/list_by_mbid', {
                                        mbid: recordingId()
                                    });
                                    const invalidTrack = acoustidForRecording.tracks.find(track => {
                                        const id = track.id;
                                        if (id === acoustid) {
                                            return false;
                                        }
                                        else {
                                            const result: AcoustIdMetaData = acoustIdGet('lookup', {
                                                meta: 'recordingids',
                                                trackid: id
                                            });
                                            const results = result.results;
                                            assertEquals(results.length, 1);
                                            const firstResult = results[0];
                                            assertEquals(firstResult.id, id);
                                            const recordings = firstResult.recordings;
                                            assertDefined(recordings);
                                            const rlen = recordings!.length;
                                            assert(rlen !== 0);
                                            return rlen !== 1;
                                        }
                                    })
                                    return onDefined(invalidTrack, dit => {
                                        const rid = recordingId();
                                        log(`  acoustid ${dit.id} is possibly incorrectly assigned to recording ${rid}. Correct acoustid is ${acoustid}`);
                                        openRecording(`${rid}/fingerprints`);
                                        moveToNext();
                                        return false;
                                    });
                                },
                                () => {
                                    fail();
                                    const mbid = trackId as string;
                                    return searchOptMBEntity('track', mbid, [
                                        statement(db.v('entity'), 'preferred', db.v('preferred'))
                                    ], fail);
                                },
                                //fail, // check file associated track is preferred track, if not re-tag with preferred track
                                () => {
                                    if (metaData.format.container !== 'flac') {
                                        logError('  Not flac encoding! Consider to replace file with lossless flac encoding')
                                    }
                                    log('  Please check tagging in picard');
                                    moveToNext();
                                    return false;
                                }
                            ]);
                        case '.jpg':
                            remove('deleting file', 'unlink', entryPath);
                            return true;
                        default:
                            // *.m3u8
                            logError('  unknown file type!');
                            return false;
                    }
                }, () => handleNotMountedVolume(vPath, () => {
                    ifDefined(getReferencesToCurrent('directory')(), () => {
                        log('  keeping missing entry, still referenced');
                        moveToNext();
                    }, () => deleteCurrentTask('missing'));

                }))
            );
        case 'mb:artist':
            return processMBCoreEntity<Artist>('artist', [
                processAttributeHandler('type'), //artist => processAttribute(artist, 'type', 'artist'),
                processAttributeHandler('name'), //artist => processAttribute(artist, 'name', 'artist'),
                processAttributeHandler('disambiguation'), //artist => processAttribute(artist, 'disambiguation', 'artist'),
                processAttributeHandler('sort-name'),
                artist => enqueueMBEntity(artist, 'area')(),
                artist => processAttribute(artist['life-span'], 'begin', 'artist'),
                artist => processRelations(artist.relations)(),
            ]);
        // TODO: browse recoring?artist=
        case 'mb:area':
            return getMBCoreEntity<Area>('area', ['aliases', 'tags'], area => processHandlers([
                attributeHandler(area, 'type', 'area'),
                attributeHandler(area, 'name', 'area'),
                /// TODO: Common code
                () => onDefined(area["iso-3166-1-codes"], codes => enqueueNextItemTask(codes, 'mb:area-iso1', 'mb:area-iso1', undefined, true, () => undefined)),
                handleAttributes<Area, Alias>(area, 'aliases', 'name', 'area', 'alias'),
                //() => enqueueNextTypedTask(area.aliases, alias => alias.name, 'mb:area-alias', 'mb:area-alias', undefined, true, () => undefined),
                processRelations(area.relations)
            ]));
        case 'mb:release':

            return getMBCoreEntity<Release>('release', ['artists', 'release-groups', 'media', 'labels', 'recordings'], release => {

                function releaseAttributeHandler(attribute: LiteralPropertyName<Release>): () => boolean | undefined {
                    return attributeHandler(release, attribute, 'release')
                }

                return processHandlers([
                    releaseAttributeHandler('title'),
                    processArtistCredits(release),
                    enqueueMBEntity(release, 'release-group'),
                    () => {
                        for (const medium of release.media) {
                            if (enqueueMedium<boolean>(release.id, medium.position, false)) {
                                return true;
                            }
                        }
                        return undefined;
                    },
                    processRelations(release.relations),

                    releaseAttributeHandler('status'),
                    attributeHandler(release['text-representation'], 'language', 'release'),
                    attributeHandler(release['text-representation'], 'script', 'release'),
                    releaseAttributeHandler('date'),
                    processEntityList<Release, ReleaseEvent>(release, "release-events", 'area'),
                    releaseAttributeHandler('barcode'),
                    releaseAttributeHandler('asin'),
                    handleAttributes<Release, LabelInfo>(release, 'label-info', 'catalog-number', 'release', 'catno'),
                    processEntityList<Release, LabelInfo>(release, 'label-info', 'label'),
                    // enqueue label
                    releaseAttributeHandler('country'),
                    fail,
                    () => {
                        // create tracklist
                        return fail();
                        /*
                        const media = release.media;
                        let currentMedia = media.length;
                        if (currentMedia === 0) {
                            return fail();
                        }
                        else {
                            currentMedia--;
                            let currentTrack = media[currentMedia].tracks.length - 1;
            
            
                            return fxail();
                        }
                        */
                    }
                ])
            });

        case 'mb:recording':

            return getMBCoreEntity<Recording>('recording', ['artist-credits', 'isrcs'], recording =>

                processHandlers([
                    attributeHandler(recording, 'title', 'recording'),
                    processArtistCredits(recording),
                    () => browse('release', 'recording', recording),
                    processRelations(recording.relations),
                    //processEntityList<Recording, ArtistCredit>(recording, 'artist-credit', 'artist'),
                    () => {
                        const result: AcoustIdTracks = acoustIdGet(`track/list_by_mbid`, {
                            mbid: recording.id
                        });
                        return enqueueNextIdTask<AcoustIdTracks, AcoustIdRecording>(result, "tracks", rec => rec.id, 'acoustid')
                        //fail();
                    },
                    attributeHandler(recording, 'length', 'recording'),
                    () => enqueueNextIdTask<Recording, string>(recording, "isrcs", isrc => isrc, 'isrc'),
                    () => {
                        const next = getReferencesToCurrent('recording');
                        const fso1 = next();
                        return ifDefined(fso1, () => {
                            log(`  recording in file ${getPath(fso1.subject)}`);
                            return undefined;
                        }, () => {
                            // no recording found
                            logError(`  missing recording ${url('musicbrainz.org', 'recording', recording.id)}!`);
                            openRecording(recording.id);
                            moveToNext();
                            return false;
                        });
                    },

                    () => {
                        log('  completed. Please verify!');
                        openRecording(recording.id);
                        //fail();
                        moveToNext();
                        return false;
                    }
                ]));
        case 'mb:label':
            fail();
            /*
            const mbid = getStringProperty('mb:mbid');
            const resourcePath = `/ws/2/${type}/${mbid}?inc=releases`;
            log(`https://musicbrainz.org${resourcePath}`);
            const resp = httpsGet('musicbrainz.org', 1000, resourcePath)
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
                type: expectEquals("Original Production"),
                'type-id': expectEquals("7aaa37fe-2def-3476-b359-80245850062d")
            }), concat(
                nameTags(expectEquals('Arista'), 'Arista'),
                concat(
                    expectPlainTextTag('label-code', '3484'),
                    expectUsCountry,
 
 
                    expectArea('489ce91b-6658-3307-9877-795b68554c98', 'United States', expectIsoList1('US')),
                    expectSimpleTag('life-span', concat(
                        expectPlainTextTag('begin', '1974'),
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
                )
            )));
 
 
            for (; ;) {
                if (isUndefined(predicates)) {
                    enqueueMBResourceTask('489ce91b-6658-3307-9877-795b68554c98', 'area', fail);
                    break;
                }
                log(asString(predicates.first));
                if (predicates.first(nextEvent())) {
                    break;
                }
                predicates = predicates.rest;
            }
            */

            //}
            return true;
        case 'acoustid':
            const acoustid = getStringProperty('acoustid');
            log(url('acoustid.org', 'track', acoustid));
            const result: AcoustIdMetaData = acoustIdGet('lookup', {
                meta: 'recordingids',
                trackid: acoustid
            });
            const results = result.results;
            assertEquals(results.length, 1);
            const firstResult = results[0];
            assertEquals(firstResult.id, acoustid);
            //const recordings = firstResult.recordings;
            return ifDefined(firstResult.recordings, recordings => {
                failIf(isEmpty(recordings));
                return processHandlers([
                    () => {
                        for (const item of recordings) {
                            const recordingId = item.id;
                            if (mbGet(`recording/${item.id}`, {}, () => false, () => true, () => false)) {
                                log(`  please deactivate deleted recording ${recordingId}`);
                                openBrowser('acoustid.org', 'track', acoustid);
                                moveToNext();
                                return false;
                            }
                        }
                        return undefined;
                    },
                    () => processNextEntityFromList(recordings, 'recording'),
                    () => {
                        moveToNext();
                        if (recordings.length === 1) {
                            log("  completed: no merge potential");
                            //openBrowser('acoustid.org', 'track', acoustid);
                            return true;

                        }
                        else {
                            //fail();
                            log("  check possible merges");
                            openBrowser('acoustid.org', 'track', acoustid);
                            return false;
                        }
                    }
                ]);
            }, () => {
                deleteCurrentTask("no recordings associated to acoustid");
                return true;
            });
        case 'mb:track': {
            const mbid = getStringProperty('mb:mbid');
            log(url('musicbrainz.org', 'track', mbid));
            return getReleaseForTrack(mbid, '+discids', release => {
                const releaseId = release.id;
                const medium = getMediumForTrack(release, mbid);
                const position = (medium as Medium).position;
                const track = findTrack(medium as Medium, mbid) as Track;
                return processHandlers([

                    () => enqueueMedium(releaseId, position, undefined),
                    enqueueMBEntity(track, 'recording'),
                    processArtistCredits(track),
                    //fail, // enqueue track-artists, track-title?
                    () => {
                        const discs = medium.discs;
                        if (discs.length === 0) {
                            return undefined;
                        }
                        else {
                            let lengthSum = 0;
                            const position = track.position;
                            for (const disc of discs) {
                                const offsets = disc.offsets;
                                lengthSum += (position === offsets.length ? disc.sectors : offsets[position]) - offsets[position - 1];
                            }
                            const length = div(lengthSum, 75 * discs.length);
                            if (div(track.length, 1000) === length) {
                                return undefined
                            }
                            else {
                                logError(`  Inaccurate track length. Track length according discid's should be ${format(length * 1000)}.`);
                                openTrack(mbid);
                                //moveToNext();
                                return false;
                            }
                        }

                    },

                    // search file system entry for track
                    () => {
                        const fso = db.v('fso');
                        const nextFile = prepareDBStream(db.searchStream([
                            statement(fso, 'type', 'fileSystemEntry'),
                            statement(fso, 'track', mbid),
                            //statement(recording, 'mb:mbid', encodeString(track.recording.id))
                        ]));
                        return ifDefined(nextFile(), fail, () => {
                            // no file found, mark as not in collection

                            return notInCollection(() => {

                                // already marked as not in collection
                                // let's see if we can restruct one from another track
                                const recording = db.v('recording');
                                const nextRFile = prepareDBStream(db.searchStream([
                                    statement(fso, 'type', 'fileSystemEntry'),
                                    statement(fso, 'recording', recording),
                                    statement(recording, 'mb:mbid', encodeString(track.recording.id))
                                ]));
                                return ifDefined(nextRFile(), (data) => {
                                    // found at least one file with same recording
                                    log('  missing track could be constructed from one of following files:')
                                    for (; ;) {
                                        log(`  * ${getPath((data as any).fso)}`);
                                        data = nextRFile();
                                        if (isUndefined(data)) {
                                            break;
                                        }
                                    }
                                    openTrack(track.id);
                                    return false;
                                }, () => undefined // no recording found => ignore, since recording task already enqueued
                                );

                            });
                        });
                    },






                    // if preferred track is current track then
                    //   check for music files with current track
                    //   case number of found music files
                    //   0: find musicfile with same recording, duiplicate and retag
                    //   1: // everithing ok
                    //   default:
                    //     duplicate found -> delete
                    //   end case
                    // endif
                    () => {
                        fail();
                        return false;
                        /*
                        return getObject(currentTask, 'preferred', () => {
                            const nextFile = getReferencesToCurrent('track');
                            return ifDefined(nextFile(), fail, () => {
                                const fso = db.v('fso');
                                const recording = db.v('recording');
                                const nextRFile = prepareDBStream(db.searchStream([
                                    statement(fso, 'type', 'fileSystemEntry'),
                                    statement(fso, 'recording', recording),
                                    statement(recording, 'mb:mbid', encodeString(track.recording.id))
                                ]));
                                let data = nextRFile();
                                return ifDefined(data, () => {
                                    log('  missing track could be constructed from one of following files:');
                                    for (; ;) {
                                        log(`  * ${getPath((data as any).fso)}`);
                                        data = nextRFile();
                                        if (isUndefined(data)) {
                                            break;
                                        }
                                    }
                                    openTrack(track.id);
                                    return false;
                                }, () => undefined);
                            });
                        }, fail)
                        */
                    },

                    /*
                    () => {
                        // check multiple files for track
                        assertUndefined(getReferencesToCurrent("track")());
                        // ensure file exist if preferred
                        getObject(currentTask, "preferred", fail, fail);
                        
                        if (isDefined(getReferencesToCurrent("track")())) {
                            fail();
         
                        }
                        else {
                            fail();
                        }
                        //next()
                        
         
                        return fail();
                    },
                    */
                    () => {
                        fail();
                        return false;
                    },
                    COMPLETED_HANDLER
                ]);

            }, () => {
                deleteCurrentTask("track does not exist anymore in musicbrainz database");
                return true;
            });

        }
        case 'mb:medium':
            {
                const mposition = getNumberProperty('mb:position');
                //assertType (mposition, "number");
                return getMBEntity<Release>('release', {
                    inc: 'recordings+discids'

                }, 'mb:release-id', mbid => `${mbid}/disc/${mposition}`, rel => {
                    const medium: Medium = rel.media.find(m => m.position == mposition) as Medium;
                    assertDefined(medium);
                    return processHandlers([
                        () => enqueueMBEntityId(getStringProperty('mb:release-id'), 'release', () => undefined),
                        attributeHandler(medium, 'format', 'medium'),
                        processListAttribute<Medium, Disc>(medium, "discs", disc => disc, 'discid'),
                        processListAttribute<Medium, Track>(medium, "tracks", track => track, 'track'),
                        () => {
                            // check completeness
                            for (const track of medium.tracks) {
                                //fail();
                                const tr = db.v('track')
                                if (searchOpt([
                                    statement(tr, 'type', 'mb:track'),
                                    statement(tr, 'mb:mbid', track.id),
                                    statement(tr, 'in-collection', 'true')
                                ], () => true, fail)) {
                                    if (notInCollection(() => false)) {
                                        return true;
                                    };
                                }
                            }
                            //updateObject('in-collection', 'true', fail)
                            fail();
                            return undefined;
                        },
                        fail,
                        COMPLETED_HANDLER
                    ]);
                });
            }
        case 'mb:work':
            return processMBCoreEntity<Work>('work', [
                processAttributeHandler('title'),
                processAttributeHandler('type'),
                work => processRelations(work.relations)(),
                work => browse('recording', 'work', work),
                work => {
                    for (const lang of work.languages) {
                        if (tryEnqueueTypedTopLevelTask(lang, mb(`work-language`))) {
                            return true
                        }
                    }
                    return undefined;
                },
                work => {
                    openMB('work', work.id);
                    completed();
                    return false;
                }
            ]);
        case 'mb:area-type':
            return processDefaultSearch<AreaList, Area>('area', 'type', 'areas', fail);
        case 'mb:artist-type':
            return processDefaultSearch<ArtistList, Artist>('artist', 'type', 'artists', (artist, searchValue) => artist.type === searchValue);
        case 'mb:release-status':
            return processDefaultSearch<ReleaseList, Release>('release', 'status', 'releases', fail);
        case 'mb:release-quality':
            return processDefaultSearch<ReleaseList, Release>('release', 'quality', 'releases', fail);
        case 'mb:release-title':
            return processEntityTypeSearch<ReleaseList, Release>('release', 'title', 'releases', (release, searchValue) => release.title === searchValue);
        case 'mb:recording-title':
            return processEntityTypeSearch<RecordingList, Recording>('recording', 'title', 'recordings', (recording, title) => recording.title === title);
        case 'mb:artist-name':
            //fail();
            // should search artist field and not name field
            // search string needs quote
            //return processDefaultSearch('artist', 'name');
            return processEntityTypeSearch<ArtistList, Artist>('artist', 'name', 'artists', (artist, searchValue) => artist.name === searchValue);
        case 'mb:area-name':
            return processEntityTypeSearch<AreaList, Area>('area', 'name', 'areas', (area, searchValue) => area.name === searchValue);
        case 'mb:release-language':
            return processEntitySearch<ReleaseList, Release>('release', 'language', 'lang', 'releases', (release, searchValue) => release['text-representation'].language === searchValue);
        case 'mb:medium-format':
            return processStringSearch<ReleaseList, Release>('release', 'format', 'format', 'medium', 'releases', (release, searchValue) => isDefined(release.media.find(media => media.format === searchValue)));
        case 'mb:work-title':
            return processEntityTypeSearch<WorkList, Work>('work', 'title', 'works', (work, searchValue) => work.title === searchValue);
        case 'mb:recording-length':
            return processSearch<RecordingList, Recording>('recording', 'length', 'n', 'dur', 'recording', 'recordings', (recording, numberString) => recording.length === Number(numberString));
        case 'mb:release-script':
            return processDefaultSearch<ReleaseList, Release>('release', 'script', 'releases', fail);
        case 'mb:artist-begin':
            return processDefaultSearch<ArtistList, Artist>('artist', 'begin', 'artists', (artist, searchValue) => artist["life-span"].begin === searchValue);
        case 'mb:area-iso1':
            return processDefaultSearch<AreaList, Area>('area', 'iso1', 'areas', (area, searchValue) => {
                const codes = area['iso-3166-1-codes'];
                return isDefined(codes) && isDefined(codes.find(code => code === searchValue));
            });
        case 'mb:release-date':
            return processDefaultSearch<ReleaseList, Release>('release', 'date', 'releases', (release, searchValue) => release.date === searchValue);
        case 'mb:area-alias':
            return processDefaultSearch<AreaList, Area>('area', 'alias', 'areas', (area, searchValue) => isDefined(area.aliases.find(alias => alias.name == searchValue)));
        case 'mb:release-barcode':
            return processDefaultSearch<ReleaseList, Release>('release', 'barcode', 'releases', (release, searchValue) => release.barcode === searchValue);
        case 'mb:discid':
            return getTypedMBEntity<Disc>('mb:mbid', 'cdtoc', 'discid', {}, id => id, disc => {
                const releases = disc.releases;
                return processHandlers([
                    () => processNextEntityFromList(releases, 'release'),
                    () => {
                        //failIf(disc.releases.length !== 1);
                        if (releases.length !== 1) {
                            for (const release of releases) {
                                for (const media of release.media) {
                                    if (isDefined(media.discs.find(discid => discid.id === disc.id))) {
                                        if (media.discs.length !== 1) {
                                            log(`  disc id can be removed from ${release.id}`);
                                            moveToNext();
                                            return false;
                                        }
                                    }
                                    else {
                                        fail();
                                    }
                                }
                            }
                        }
                        return undefined;
                        // check releases, whether or not have additional disc ids, would indicate wrong addition
                    },
                    COMPLETED_HANDLER
                ]);
            });
        case 'mb:tag':
            const key = decodeStringLiteral(getPropertyFromCurrent(mb(`tag`)));
            //log(`https://musicbrainz.org/tag/${encodeURIComponent(decodeLiteral(getPropertyFromCurrent(mb(`tag`)), 's'))}`);
            //return handleSearch(`tag/${encodeURIComponent(key)}`, escapeLucene(key), 'area', 'tag', fail);

            //function handleSearch(path: string, escapedKey: string, type: string, searchField: string, completed: (entities: Entity[]) => boolean): boolean {
            log(url('musicbrainz.org', 'tag', encodeURIComponent(key)));
            //`https://musicbrainz.org/${path}`);
            //const escapedKey = escapeLucene(key);
            failIf(isEmpty(key));
            /*
            const list: EntityList = mbGetList('area', { query: `tag:${escapeLucene(key)}` });
            const count: number = list.count;
            failIf(count === 0);
            const entities = (list as AreaList).areas;
            return enqueueNextEntityFromList(entities, 'area', () => {
                assert(count === entities.length);
                return fail();
            });
            */

            function tagUsageHandler(type: string): () => boolean | undefined {
                return () => {
                    const list: EntityList = mbGetList(type, { query: `tag:${escapeLucene(key)}` }, 0);
                    const count: number = list.count;
                    failIfZero(count);
                    const entities = (list as any)[`${type}s`];
                    return enqueueNextEntityFromList(entities, type, () => {
                        assert(count === entities.length);
                        return undefined;
                        //return fail();
                    });
                }

            }
            return processHandlers([
                tagUsageHandler('area'),
                tagUsageHandler('artist'),
                /*
                () => {
                    const list: EntityList = mbGetList('area', { query: `tag:${escapeLucene(key)}` });
                    const count: number = list.count;
                    failIf(count === 0);
                    const entities = (list as AreaList).areas;
                    return enqueueNextEntityFromList(entities, 'area', () => {
                        assert(count === entities.length);
                        return undefined;
                        //return fail();
                    });
                },
                () => {
                    const list: EntityList = mbGetList('artist', { query: `tag:${escapeLucene(key)}` });
                    const count: number = list.count;
                    failIf(count === 0);
                    const entities = (list as ArtistList).artists;
                    return enqueueNextEntityFromList(entities, 'artist', () => {
                        assert(count === entities.length);
                        return undefined;
                        //return fail();
                    });
                }
                */
            ])
        case 'mb:release-asin':
            return processDefaultSearch<ReleaseList, Release>('release', 'asin', 'releases', (release, searchValue) => release.asin === searchValue);
        case 'mb:release-catno':
            return processDefaultSearch<ReleaseList, Release>('release', 'catno', 'releases', (release, searchValue) => isDefined(release['label-info'].find(labelInfo => labelInfo['catalog-number'] === searchValue)));
        /*
        case 'mb:mb:discid':
            deleteCurrentTask('invalid: mb:mb:discid');
            return true;
            */
        case 'playlist':
            log('playlist');
            deleteCurrentTask("not anymore used");
            return true;
        //return fail();
        case 'mb:url':
            return processMBCoreEntity<URL>('url', [
                url => {
                    openMB("url", url.id);
                    log("  please check external data");
                    moveToNext();
                    return false;
                },
            ]);
        //fail();
        //return false;
        case 'mb:release-group':
            return processMBCoreEntityInc<ReleaseGroup>('release-group', ["artists", "releases"], [
                processAttributeHandler('title'),
                releaseGroup => processArtistCredits(releaseGroup)(),
                releaseGroup => processNextEntityFromList(releaseGroup.releases, 'release'),
                releaseGroup => processRelations(releaseGroup.relations)(),

            ]);
        //return fail();
        //return getMBCoreEntity<ReleaseGroup>('release-group', [], releaseGroup => processHandlers())
        case 'mb:mb:tag':
            fail()
            deleteCurrentTask('invalid type');
            return false;
        //return fail();
        case 'mb:artist-disambiguation':
            return processEntitySearch<ArtistList, Artist>('artist', 'disambiguation', 'comment', 'artists', (artist, searchValue) => artist.disambiguation === searchValue);
        //return fail();
        case 'url':
            deleteCurrentTask("not anymore supported");
            /*
            const urlp = getStringProperty('url');
            log(urlp);
            browser(urlp);
            */
            return true;
        case 'mb:release-group-title':
            return processEntityTypeSearch<ReleaseGroupList, ReleaseGroup>('release-group', 'title', 'release-groups', (releaseGroup, searchValue) => releaseGroup.title === searchValue);
        case 'mb:artist-sort-name':
            return processEntitySearch<ArtistList, Artist>('artist', 'sort-name', 'sortname', 'artists', (artist, searchValue) => artist['sort-name'] === searchValue);
        case 'mb:work-type':
            return processDefaultSearch<WorkList, Work>('work', 'type', 'works', (work, searchValue) => work.type === searchValue);
        default:
            console.error(type);
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


defineCommand<StatementPattern>("get", "retrieve triples from database", ["-s, --subject <IRI>", "-p, --predicate <IRI>", "-o, --object <IRI>"], options => {
    getAll(options.subject, options.predicate, options.object, log);
});


defineCommand("browse <uri>", "browse URI, shows triples where URI is used either as subject, predicate or object", [], uri => {
    var listed = immutable.Set();
    function browse(subject: any, predicate: any, object: any) {
        getAll(subject, predicate, object, line => {
            if (!listed.contains(line)) {
                log(line);
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
    defineCommand<string>(`${cmdName} <${cmdName}spec>`, description, [], (spec) => {
        const res: Pair<NodeJS.ErrnoException | null, string> = waitFor2(callback => fs.readFile(spec, 'utf8', callback));
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
        data: (data: any) => log(query.select.map((field: string) => {
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
                log(`${type} subject: ${s.subject} predicate=${s.predicate} object=${s.object}`)
                streams[type].write(statement);
            });
            putStream.end();
            delStream.end();
            log('processed!')
        }
    });
});


defineCommand("purge", "removes all triples from the database, empties the database", [], () => {
    db.close();
    assert(isNull(waitFor(callback => rimraf(dbPath, callback))));
});

function enqueue(type: string, id: string) {
    tryAdd(id, type, type, undefined, '', undefined, () => undefined, () => log(`already added ${type} ${id}`));
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
                    log(`ignore file: ${nl.substr(4)}`);
                }
                else {
                    if (nl === '<root>') {
                        log('ignore root')
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
    function decodeLiteral(literal: string): string | number {
        const segments = literal.split('/');
        assertEquals(segments.length, 2);
        const value = decodeURIComponent(segments[1]);
        switch (segments[0]) {
            case 'n':
                return Number(value);
            case 's':
                return value;
            default:
                return fail();
        }
    }

    /*
    function getString(name: string): string {
        return getLiteral(name, 's');
    }
    */

    function url(domain: string, path: string) {
        log(`https://${domain}/${path}`);
    }

    function getLiteral(name: string): string | number {
        return decodeLiteral(getPropertyFromCurrent(name));
    }

    function resource(domain: string, type: string, predicate: string): void {
        url(domain, `${type}/${getLiteral(predicate)}`);
    }

    /*
    function musicbrainz(type: string, predicate: string): void {
        resource('musicbrainz.org', type, predicate);
    }
    */
    function mbResource(type: string): void {
        resource('musicbrainz.org', type, 'mb:mbid');
    }

    function searchBase(field: string, prefix: string, suffix: string, type: string) {
        url('musicbrainz.org', `search?query=${encodeURIComponent(`${field}:"${escapeLucene(getLiteral(`mb:${prefix}-${suffix}`))}"`)}&type=${type}&method=advanced`);
    }

    function searchMB(field: string, type: string, suffix: string) {
        searchBase(field, type, suffix, type);
    }

    function search(field: string, type: string) {
        searchMB(field, type, field);
    }

    function searchType(type: string) {
        search('type', type);
    }

    for (; ;) {


        const type = getPropertyFromCurrent('type');
        switch (type) {
            case 'acoustid':
                resource('acoustid.org', 'track', 'acoustid');
                break;
            case 'fileSystemEntry':
                log(getPath(current));
                break;
            case 'volume':
                log(getLiteral('path'));
                break;
            case 'root':
                log('<root>');
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
                log(`https://musicbrainz.org/release/${getLiteral('mb:release-id')}/disc/${getLiteral('mb:position')}`);
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
            case 'mb:release-status':
                search('status', 'release');
                break;
            case 'mb:release-quality':
                search('quality', 'release');
                break;
            case 'mb:release-title':
                searchMB('release', 'release', 'title');
                break;
            case 'mb:recording-title':
                searchMB('recording', 'recording', 'title');
                break;
            case 'mb:artist-name':
                searchMB('artist', 'artist', 'name');
                break;
            case 'mb:area-name':
                searchMB('area', 'area', 'name');
                break;
            case 'mb:release-language':
                searchMB('lang', 'release', 'language');
                break;
            case 'mb:medium-format':
                searchBase('format', 'medium', 'format', 'release');
                break;
            case 'mb:work-title':
                searchMB('work', 'work', 'title');
                break;
            case 'mb:recording-length':
                searchMB('dur', 'recording', 'length');
                break;
            case 'mb:release-script':
                search('script', 'release');
                break;
            case 'mb:artist-begin':
                search('begin', 'artist');
                break;
            case 'mb:area-iso1':
                search('iso1', 'area');
                break;
            case 'mb:release-date':
                search('date', 'release');
                break;
            case 'mb:area-alias':
                search('alias', 'area');
                break;
            case 'mb:release-barcode':
                search('barcode', 'release');
                break;
            case 'mb:discid':
                mbResource('cdtoc');
                break;
            case 'mb:mb:tag':
                console.log('mb:mb:tag');
                break;
            case 'mb:tag':
                resource('musicbrainz.org', 'tag', 'mb:tag');
                break;
            case 'mb:release-asin':
                search('asin', 'release');
                break;
            case 'mb:release-catno':
                search('catno', 'release');
                break;
            case 'playlist':
                console.log('playlist');
                break;
            case 'mb:url':
                mbResource('url');
                break;
            case 'mb:release-group':
                mbResource('release-group');
                break;
            case 'mb:label':
                mbResource('label');
                break;

            /*
            case 'mb:mb:tag':
            console.log ('mb:mb:tag');
            break;
            */

            case 'mb:artist-disambiguation':
                search('disambiguation', 'artist');
                break;

            case 'url':
                log(getLiteral('url'));
                //                fail();
                break;
            case 'mb:release-group-title':
                search('title', 'release-group');
                break;
            case 'mb:artist-sort-name':
                search('sort-name', 'artist');
                break;
            case 'mb:work-type':
                search('type', 'work');
                break;
            case 'mb:release-country':
                search('country', 'release');
                break;
            case 'isrc':
                mbResource('isrc');
                fail();
                break;
            default:
                console.error(`Cannot dump type ${type}!`);
                fail();
                break;
        }
        const next = getPropertyFromCurrent('next');
        if (next === first) {
            break;
        }

        current = next;
    }
});


// To include
// /Volumes/Musik/2/2 Unlimited/No Limit_ Complete Best of 2 Unlimited/f67a9109-95f3-417a-9c7e-7c9be1e3f303/04 Workaholic (vocal edit).flac

defineCommand<string>("include <path-or-url>", "include path or url into database", [], givenPath => {
    if (givenPath.startsWith("https:")) {
        log(`including url: ${givenPath}`);
        enqueueTypedTopLevelTask(givenPath, 'url', () => log('already added'))
    }
    else {
        let pn = givenPath;

        //log(givenPath);
        let segments: string[] = [];
        for (; ;) {
            if (searchSubject({ type: 'volume', path: encodeString(pn) }, [], false, res => {
                let currentResult = res;
                let currentPath = pn;
                for (; ;) {

                    failIfEmpty(segments);
                    const name = segments[0];
                    currentPath = path.join(currentPath, name);


                    function mapAttributeValues<T>(mapper: BiFunction<string, string, T>): T[] {
                        return mapDictionary({
                            type: 'fileSystemEntry',
                            directory: currentResult.entity,
                            name: encodeString(name),
                        }, (key, value) => mapper(key, value));
                    }
                    const newResult = searchOpt(mapAttributeValues((predicate, object) => statement(db.v('entity'), predicate, object)), () => undefined, res => res);
                    if (isUndefined(newResult)) {
                        // TODO enqueue name
                        log('must enque: ')
                        log(name);
                        //fail();

                        log(`adding fileSystemEntry ${currentPath}`);
                        const taskId = `task/${uuid()}`;


                        const currentTask = getCurrentTask();

                        update([
                            ...mapAttributeValues((key, value) => put(taskId, key, value)),
                            link(taskId, currentTask),
                            ...appendToPrev(taskId),
                        ]);
                        break;


                    }
                    segments = segments.slice(1);
                    //res = newResult;
                    currentResult = newResult;

                }
                return true;

            })) {
                break;
            }
            //pn = path.dirname(pn);
            segments = [path.basename(pn), ...segments];
            pn = path.dirname(pn);
            failIfSame(pn, '.');//fail();

        }
        //fail();

    }
});

defineCommand('remove', 'remove current task from queue', [], () => {
    removeCurrent();
});

defineCommand('skip', 'skip current task', [], moveToNext);

Fiber(() => {

    commander.parse(process.argv);
    if (!executed) {
        commander.outputHelp();
    }

}).run();
