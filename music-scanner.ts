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
import * as http from 'http';



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

function decodeLiteral(literal: string): string | number {
    const segments = literal.split('/');
    assertEquals(segments.length, 2);
    const value = decodeURIComponent(segments[1]);
    switch (segments[0]) {
        case 's':
            return value;
        case 'n':
            return Number(value);
        default:
            return fail();
    }
    //assertEquals(segments[0], prefix);
    //return decodeURIComponent(segments[1]);
}


function decodeStringLiteral(stringLiteral: string): string {
    const result = decodeLiteral(stringLiteral);
    assertType(result, "string");
    return result as string;
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


function matchObject<T>(checkers: Matcher<T>): Predicate<T> {
    const res = (object: T) => {
        //return searchMatch(object, checkers, () => true, FALSE);
        for (const compareValue of compareObjects(object, checkers)) {
            const checker = compareValue.expected as Predicate<any>;
            assertType(checker, 'function');
            if (checker(compareValue.actual)) {
                return true;
            }
        }
        return false;

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

type Attributes = Dictionary<string>;

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


function tryAddKeyedTask<T>(type: string, keys: Dictionary<string>, path: string, prefix: string, linkPredicate: string | undefined, enqueued: () => T, alreadyAdded: (found: any) => T): T {

    const currentTask = getCurrentTask();


    function mapAttributeValues<S, T>(mapper: (subject: S, predicate: string, object: string) => T, subject: S): T[] {

        return mapDictionary({
            type: type,
            ...keys
        }, (key, value) => mapper(subject, key, value));
    }

    return streamOpt(db.searchStream(mapAttributeValues(statement, db.v('s'))), () => {
        log(`${prefix}adding ${type} ${path}`);
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
    return tryAddKeyedTask(type, keys, `${key}`, prefix, linkPredicate, enqueued, alreadyAdded);
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

interface Area {
    readonly id: string;
    readonly type: string;
    readonly name: string;
    readonly 'iso-3166-1-codes': string[];
    readonly aliases: Alias[];
}

interface Lifespan {
    readonly begin: string;
}

interface Relation {
    readonly artist?: Entity;
    readonly work?: Entity;
    readonly recording?: Entity;
    readonly 'target-type': string;
}

interface Artist {
    readonly id: string;
    readonly area: Area;
    readonly type: string;
    readonly name: string;
    readonly 'life-span': Lifespan;
    readonly relations: Relation[];
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
}

interface Track {
    readonly id: string;
    readonly recording: Recording;
    readonly title: string;
    readonly "artist-credit": ArtistCredit[];
    readonly position: number;
}

interface Disc {
    readonly id: string;
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
    area: Entity | null;
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
}

interface ReleaseList extends EntityList {
    readonly "release-count": number;
    readonly releases: Release[];
}

interface RecordingList extends EntityList {
    readonly recordings: Recording[];
}

interface Work {
    readonly id: string;
    readonly title: string;
    readonly relations: Relation[];
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

interface AcoustIdRecording {
    readonly id: string;
}

interface AcoustIdTrack {
    readonly id: string;
    readonly recordings: AcoustIdRecording[];
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

type ConformMembers<T, C> = { [K in keyof T]: T[K] extends C ? K : never };

type ConformPropertyName<T, C> = ConformMembers<T, C>[keyof T];

type LiteralPropertyName<T> = ConformPropertyName<T, string | number | null | undefined>;

//type ArrayPropertyName<T> = ConformPropertyName<T, any[]>;

//type ElementType<T> = T extends (infer E)[] ? E : never;

//type APRelease = ArrayPropertyNames<Release>;

//type Arelease = Release[APRelease];

/*
type StringPropertyName<T> = ConformPropertyName<T, string>;
*/

//type Element<T> = T extends (infer E)[] ? E : never;
//type A = Element<Entity[]>;

/*
type ProcessEntityList<T > ={
    collection: ConformPropertyName<T, any[]>;
    elementName: ConformPropertyName<ConformPropertyMembers<T, any[]>, Entity>
};

//type PELR = ProcessEntityList<Release>;

const pelr: ProcessEntityList<Release> {
    collection: ""
}
*/

// = (source: T, collection: ConformPropertyName<T, (infer E)[]>, elementName: ConformPropertyName<E, Entity>, type: string) => (() => boolean | undefined)
//console.log('a+-&&||!(){}[]^"~*?:\\/bcd');
//console.log('a+-&&||!(){}[]^"~*?:\\/bcd'.replace(/[!"()&+-:]|\||\{|\}|\[|\]|\^|\~|\*|\?|\\|\//g, s => `\\${s}`));

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

function openBrowser(server: string, type: string, id: string): void {
    opn(url(server, type, id), { wait: false });
}

function escapeLucene(key: string | number): string {
    //  console.log(`${key}`.replace(/[!"()&+:]|\||\{|\}|\[|\]|\^|\~|\*|\?|\\|\/|\-/g, s => `\\${s}`));
    return `${key}`.replace(/[!"()&+:]|\||\{|\}|\[|\]|\^|\~|\*|\?|\\|\/|\-/g, s => `\\${s}`);
}

function mb(name: string): string {
    return `mb:${name}`;
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

    function fsOp(removeMethod: 'unlink' | 'rmdir', path: string) {
        assertSame(waitFor(cb => fs[removeMethod](path, cb)), null);
    }

    function remove(message: string, removeMethod: 'unlink' | 'rmdir', path: string): void {
        log(`  ${message}`);
        fsOp(removeMethod, path);
        //assertSame(waitFor(cb => fs[removeMethod](path, cb)), null);
        moveToNext();
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

    function enqueueNextTask<T, R>(items: T[], processTypedItem: (item: T, handler: (name: string, type: string) => boolean) => boolean, predicate: string, parentPredicate: string | undefined, foundResult: R, completed: () => R): R {
        for (const item of items) {
            //console.log(item);
            if (processTypedItem(item, (name: string, type: string) => enqueueUnlinkedTask(name, type, predicate, parentPredicate, () => false))) {
                return foundResult;
            }
        }
        return completed();
    }

    function enqueueTopLevelTask<T>(key: string | number, type: string, namePredicate: string, alreadyAdded: (id: string) => T): boolean | T {
        return enqueueUnlinkedTask(key, type, namePredicate, undefined, alreadyAdded);
    }

    function enqueueTypedTopLevelTask(key: string | number, type: string): boolean | undefined {
        return enqueueTopLevelTask(key, type, type, () => undefined);
    }

    function completed(): void {
        log('  completed');
        moveToNext();
    }

    function enqueueNextTypedTask<T, R>(items: T[], data: (item: T) => string, type: string, predicate: string, parentPredicate: string | undefined, foundResult: R, completed: () => R): R {
        return enqueueNextTask(items, (item, handler) => handler(data(item), type), predicate, parentPredicate, foundResult, () => completed())
    }

    function enqueueNextItemTask<T>(items: string[], type: string, predicate: string, parentPredicate: string | undefined, foundResult: T, completed: () => T): T {
        return enqueueNextTypedTask<string, T>(items, item => item, type, predicate, parentPredicate, foundResult, () => completed())
    }

    function enqueueTasks(items: string[], type: string, predicate: string, parentPredicate: string | undefined): void {
        enqueueNextItemTask(items, type, predicate, parentPredicate, undefined, () => completed())
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


    /*

    function updateObject(predicate: string, object: string): () => boolean | undefined {
        return () => {
            //const l = encodeLiteral('s', value);
            return getObject<boolean | undefined>(currentTask, predicate, () => {
                log(`  ${predicate}: undefined --> ${object} `);
                update([
                    put(currentTask, predicate, object),
                    ...moveToNextStatements(),
                ]);
                return true;
            }, (object: string) => {
                assertEquals(object, object);
                return undefined;
            })
        }
    }

    */


    const type = getPropertyFromCurrent('type');

    function processFileSystemPath<T>(path: string, directory: () => T, file: () => T, missing: () => T): T {
        log(`processing ${type} ${path}`);
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
        const run = getRunner();
        https.get({
            hostname: apiHost,
            path: resourcePath,
            port: 443,
            headers: { 'user-agent': 'rasuni-musicscanner/0.0.1 ( https://musicbrainz.org/user/rasuni )' }
        }, run).on("error", fail);
        return yieldValue();
    }

    function wsGet<T, R>(minimalDelay: number, path: string, params: Dictionary<string>, apiHost: string, found: (data: T) => R, notFound: () => R): R {
        let resourcePath = `/${path}`;
        const paramString = mapDictionary(params, (key, value) => `${key}=${encodeURIComponent(value)}`).join('&');
        if (paramString.length !== 0) {
            resourcePath = `${resourcePath}?${paramString}`
        }
        let retryCount = 0;
        for (; ;) {
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
                    if (retryCount === 1) {
                        return fail();
                    }
                    retryCount++;
                    break;
                default:
                    return fail();
            }
        }
    }


    function mbGet<T, R>(resource: string, params: Dictionary<string>, found: (data: T) => R, notFound: () => R): R {
        //console.log(url('musicbrainz.org', logType, mbid));
        return wsGet(1000, `ws/2/${resource}`, {
            fmt: 'json',
            ...params
        }, 'musicbrainz.org', found, notFound);
    }

    function getReleaseForTrack(trackId: string): Release {
        const metaData: ReleaseList = mbGet('release', {
            track: trackId,
            inc: "artist-credits"
        }, (data: ReleaseList) => data, fail);
        assertEquals(metaData["release-count"], 1);
        return metaData.releases[0];
    }

    function getMBEntity<T extends Entity>(type: string, params: Dictionary<string>, idPredicate: string, path: (mbid: string) => string, found: (data: T) => boolean): boolean {
        const mbid = getStringProperty(idPredicate);
        log(url('musicbrainz.org', type, path(mbid)));
        return mbGet(`${type}/${mbid}`, params, (metaData: Entity) => {
            assertEquals(metaData.id, mbid);
            //console.log(metaData.area.id);
            return found(metaData as T);
        }, () => {
            deleteCurrentTask('not found');
            return true;
        });
        /*
        assertEquals(metaData.id, mbid);
        //console.log(metaData.area.id);
        return metaData as any;
        */
    }


    function getMBCoreEntity<T extends Entity>(type: string, incs: string[], found: (data: T) => boolean): boolean {
        return getMBEntity(type, { inc: `artist-rels+recording-rels+work-rels${incs.map(inc => `+${inc}`)}` }, 'mb:mbid', path => path, found);
    }

    function enqueueNextEntityTask<T, R>(items: T[], entity: (item: T) => Entity | null, type: (item: T) => string, completed: () => R): boolean | R {
        return enqueueNextTask<T, boolean | R>(items, (item, handler) => {
            const e = entity(item);
            return e !== null && handler(e.id, mb(type(item)));
        }, 'mb:mbid', undefined, true, completed);
    }


    function enqueueMBEntityId<T>(mbid: string, type: string, alreadyAdded: () => T): boolean | T {
        return enqueueTopLevelTask(mbid, mb(type), 'mb:mbid', alreadyAdded);
    }

    function enqueueMBEntity<T>(entity: Entity, type: string, alreadyAdded: () => T): boolean | T {
        return enqueueMBEntityId(entity.id, type, alreadyAdded);
    }


    function acoustIdGet<T extends AcoustIdResult>(path: string, params: Dictionary<string>): T {
        const result: AcoustIdResult = wsGet<T, T>(334, `/v2/${path}`, {
            client: '0mgRxc969N',
            ...params
        }, 'api.acoustid.org', data => data, fail);
        assertEquals(result.status, "ok");
        return result as T;
    }


    function attributeHandler<T>(record: T, attribute: LiteralPropertyName<T>, type: string): () => undefined | boolean {
        return () => {
            const value = record[attribute] as any as string | number | null;
            return isNull(value) ? undefined : enqueueTypedTopLevelTask(value, mb(`${type}-${attribute}`));
        }
    }





    function processSearch(type: 'area' | 'artist' | 'release' | 'recording' | 'work', field: string, queryField?: string, taskType?: string): boolean {
        const escapedKey = escapeLucene(decodeLiteral(getPropertyFromCurrent(mb(`${isDefined(taskType) ? taskType : type}-${field}`))));
        const searchField = isDefined(queryField) ? queryField : field;
        log(`https://musicbrainz.org/search?query=${searchField}%3A${encodeURIComponent(escapedKey)}&type=${type}&method=advanced`);
        const list: EntityList = mbGet<EntityList, EntityList>(type, { query: `${searchField}:${escapedKey}` }, found => found, fail);
        const count: number = list.count;
        if (count === 0) {
            deleteCurrentTask('no search results');
            return true;
        }
        else {
            function process(entities: Entity[]): boolean {
                return enqueueNextEntityTask(entities, entity => entity, () => type, () => {
                    assert(count === entities.length);
                    entities.forEach(entity => searchMBEntityTask(type, entity.id, fail, taskId => getObject(taskId, 'playlist', () => undefined, fail)));
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


    function deleteCurrentTask(msg: string): void {
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
        log(`  ${msg} -> remove entry`);
        update([
            ...appendToPrev(next),
            ...setCurrent(next),
            ...updateStatements
        ]);
        assertMissing({ object: currentTask });
        assertMissing({ subject: currentTask });
    }

    function processList<T>(items: T[], entity: (item: T) => Entity | null, type: (item: T) => string): () => undefined | boolean {
        return () => enqueueNextEntityTask(items, entity, type, () => undefined);
    }


    function getNumberProperty(name: string): number {
        const res = decodeLiteral(getPropertyFromCurrent(name));
        assertType(res, "number");
        return res as number;
    }

    function processRelations(relations: Relation[]): () => boolean | undefined {
        return processList(relations, relation => {
            switch (relation["target-type"]) {
                case 'artist':
                    return relation.artist as Artist;
                case 'work':
                    return relation.work as Work;
                case 'recording':
                    return relation.recording as Recording;
                default:
                    return fail();
            }
        }, relation => relation["target-type"]);
    }

    function getReferencesToCurrent(pred: string) {
        return getStream({ predicate: pred, object: currentTask })
    }

    function processListAttribute<T, E>(source: T, collection: ConformPropertyName<T, E[]>, entity: (elem: E) => Entity | null, elementName: string): () => boolean | undefined {
        return processList(source[collection] as unknown as E[], elem => entity(elem), () => elementName);
    }

    function processEntityList<T, E>(source: T, collection: ConformPropertyName<T, E[]>, elementName: ConformPropertyName<E, Entity | null>): () => boolean | undefined {
        return processListAttribute(source, collection, elem => elem[elementName] as unknown as Entity | null, elementName as string);
        //return processList(source[collection] as unknown as E[], elem => elem[elementName] as unknown as Entity, () => elementName as string);
    }

    /*
    function processEntities<T>(source: T, collection: ConformPropertyName<T, Entity[]>, elementName: ConformPropertyName<E, Entity | null>): () => boolean | undefined {
        return processList(source[collection] as Entity[], elem => elem, () => elementName);
    }
    */

    function searchMBEntityTask<R>(type: string, mbid: string, notFound: () => R, found: (taskId: string) => R) {
        function pattern(predicate: string, object: string): Statement<any> {
            return statement(db.v('entity'), predicate, object);
        }
        return streamOpt(db.searchStream([
            pattern('type', mb(type)),
            pattern('mb:mbid', encodeString(mbid))
        ]), notFound, result => found(result.entity))
    }


    switch (type) {
        case 'root':
            log('processing root');
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
                            const trackId = common.musicbrainz_trackid;
                            assertDefined(trackId);
                            const mbid = trackId as string;
                            let recordingIdResult: string | undefined = undefined;
                            const recordingId = () => {
                                if (recordingIdResult === undefined) {
                                    const release = getReleaseForTrack(mbid);
                                    const medium = getMediumForTrack(release, mbid);
                                    const track = getTrack(medium, mbid);
                                    recordingIdResult = track.recording.id;
                                }
                                return recordingIdResult;
                            }

                            return processHandlers([
                                () => {
                                    if (isDefined(acoustid)) {
                                        return enqueueTypedTopLevelTask(acoustid, 'acoustid');
                                    }
                                    else {
                                        console.error("  acoustid is missing!");
                                        return false;
                                    };
                                },
                                () => enqueueMBResourceTask(mbid, 'track', () => undefined),
                                () => searchMBEntityTask<boolean | undefined>('recording', recordingId(), () => undefined, taskId => {
                                    return getObject<boolean | undefined>(currentTask, 'recording', () => {
                                        //const object = res.rt;
                                        log(`  recording: undefined --> ${taskId} `);
                                        update([
                                            put(currentTask, 'recording', taskId),
                                            ...moveToNextStatements(),
                                        ]);
                                        return true;
                                    }, (object: string) => {
                                        assertEquals(object, object);
                                        return undefined;
                                    });
                                }),
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
                                            const rlen = recordings.length;
                                            assert(rlen !== 0);
                                            /*
                                            if (rlen === 1) {
                                                log(`  alternative acoustid ${id} is assigned to only one recording`);
                                            }
                                            */
                                            return recordings.length !== 1;
                                        }
                                    })
                                    if (isDefined(invalidTrack)) {
                                        const rid = recordingId();
                                        log(`  acoustid ${invalidTrack.id} is possibly incorrectly assigned to recording ${rid}. Correct accoustid is ${acoustid}`);
                                        openBrowser('musicbrainz.org', 'recording', `${rid}/fingerprints`);
                                        moveToNext();
                                        return false;
                                    }
                                    else {
                                        return undefined;
                                    }
                                },
                                () => {
                                    if (metaData.format.dataformat !== 'flac') {
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
                            // *.mkv
                            logError('unknown file type!');
                            return false;
                    }


                }, () => stat(vPath, () => {
                    //const next = prepareDBStream(db.getStream({ predicate: 'directory', object: currentTask }));
                    //const data = next();

                    if (isDefined(getReferencesToCurrent('directory')())) {
                        log('  keeping missing entry, still referenced');
                        moveToNext();
                    }
                    else {
                        deleteCurrentTask('missing');
                    }

                    return true;
                }, () => {
                    //fail();
                    volumeNotMounted();
                    return false;
                }));
        case 'mb:artist':
            return getMBCoreEntity<Artist>('artist', [], artist => processHandlers([
                attributeHandler(artist, 'type', 'artist'),
                attributeHandler(artist, 'name', 'artist'),
                () => enqueueMBEntity(artist.area, 'area', () => undefined),
                attributeHandler(artist['life-span'], 'begin', 'artist'),
                processRelations(artist.relations)
            ]));
        case 'mb:area':
            return getMBCoreEntity<Area>('area', ['aliases'], area =>
                processHandlers([
                    attributeHandler(area, 'type', 'area'),
                    attributeHandler(area, 'name', 'area'),
                    () => enqueueNextItemTask(area["iso-3166-1-codes"], 'mb:area-iso1', 'mb:area-iso1', undefined, true, () => undefined),
                    () => enqueueNextTypedTask(area.aliases, alias => alias.name, 'mb:area-alias', 'mb:area-alias', undefined, true, () => undefined)
                ]));
        case 'mb:release':

            return getMBCoreEntity<Release>('release', ['artists'], release => {

                function releaseAttributeHandler(attribute: LiteralPropertyName<Release>): () => boolean | undefined {
                    return attributeHandler(release, attribute, 'release')
                }

                return processHandlers([
                    releaseAttributeHandler('title'),
                    releaseAttributeHandler('status'),
                    attributeHandler(release['text-representation'], 'language', 'release'),
                    attributeHandler(release['text-representation'], 'script', 'release'),
                    releaseAttributeHandler('date'),
                    processEntityList<Release, ReleaseEvent>(release, "release-events", 'area'),
                    processEntityList<Release, ArtistCredit>(release, 'artist-credit', 'artist'),
                    releaseAttributeHandler('barcode')
                ])
            });

        case 'mb:recording':

            return getMBCoreEntity<Recording>('recording', ['artist-credits'], recording =>

                processHandlers([
                    attributeHandler(recording, 'title', 'recording'),
                    attributeHandler(recording, 'length', 'recording'),
                    processEntityList<Recording, ArtistCredit>(recording, 'artist-credit', 'artist'),
                    processRelations(recording.relations),
                    () => {
                        const releaseList: ReleaseList = mbGet<ReleaseList, ReleaseList>('release', {
                            recording: recording.id
                        }, list => list, fail);
                        const releases = releaseList.releases;
                        assertEquals(releaseList["release-count"], releases.length);
                        return enqueueNextEntityTask(releaseList.releases, release => release, () => 'release', () => undefined);
                    },
                    () => {
                        const result: AcoustIdTracks = acoustIdGet(`track/list_by_mbid`, {
                            mbid: recording.id
                        });
                        return enqueueNextTask(result.tracks, (rec, handler) => handler(rec.id, 'acoustid'), 'acoustid', undefined, true, () => undefined)
                        //fail();
                    },
                    () => {
                        const next = getReferencesToCurrent('recording');
                        const fso1 = next();
                        if (isDefined(fso1)) {
                            const fso2 = next();
                            if (isDefined(fso2)) {
                                // duplicate files for same recording
                                // check whether acoustid's of the files are the same
                                // when not same, this would be an indication that the recordings have been merged incorrectly
                                // when not same, this would be an indication that the recordings have been merged incorrectly
                                return fail();
                            }
                            else {
                                // found single recording, everything fine, set playlist

                                return getObject<boolean | undefined>(currentTask, 'playlist', () => {
                                    //return fail();                                    
                                    const object = fso1.subject;
                                    log(`  playlist: undefined --> ${object} `);
                                    update([
                                        put(currentTask, 'playlist', object),
                                        ...moveToNextStatements(),
                                    ]);
                                    return true;
                                }, (object: string) => {
                                    object;
                                    return fail();
                                    /*
                                    assertEquals(object, object);
                                    return undefined;
                                    */
                                });

                                //return fail();
                            }
                        }
                        else {
                            // no recording found
                            logError(`  missing recording ${url('musicbrainz.org', 'recording', recording.id)}!`);
                            openBrowser('musicbrainz.org', 'recording', recording.id);
                            moveToNext();
                            return false;
                            //openBrowser()
                            //return fail();
                        }
                    }
                ]));
        case 'mb:label':
            fail();
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
            const recordings = firstResult.recordings;
            assert(recordings.length !== 0);
            return processHandlers([
                () => {
                    for (const item of recordings) {
                        const recordingId = item.id;
                        if (mbGet(`recording/${item.id}`, {}, () => false, () => true)) {
                            log(`  please deactivate deleted recording ${recordingId}`);
                            openBrowser('acoustid.org', 'track', acoustid);
                            moveToNext();
                            return false;
                        }
                    }
                    return undefined;
                },
                () => enqueueNextEntityTask(recordings, recording => recording, () => 'recording', () => undefined),
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
        case 'mb:track': {
            const mbid = getStringProperty('mb:mbid');
            log(url('musicbrainz.org', 'track', mbid));

            const release = getReleaseForTrack(mbid);
            const releaseId = release.id;

            const medium = getMediumForTrack(release, mbid);
            const position = (medium as Medium).position;
            const track = findTrack(medium as Medium, mbid) as Track;
            return processHandlers([
                () => tryAddKeyedTask('mb:medium', {
                    "mb:release-id": encodeString(releaseId),
                    "mb:position": encodeNumber(position)
                }, `${releaseId}/${position}`, '  ', undefined, () => {
                    moveToNext();
                    return true;
                }, () => undefined),
                () => enqueueMBEntity(track.recording, 'recording', () => undefined),
                () => {
                    completed();
                    return true;
                }
            ]);
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

                    ]);
                });
            }
        case 'mb:work':
            return getMBCoreEntity<Work>('work', [], work => processHandlers([
                attributeHandler(work, 'title', 'work'),
                processRelations(work.relations),
                () => {
                    openBrowser('musicbrainz.org', 'work', work.id);
                    completed();
                    return false;
                }
            ]));

        case 'mb:area-type':
            return processSearch('area', 'type');
        case 'mb:artist-type':
            return processSearch('artist', 'type');
        case 'mb:release-status':
            return processSearch('release', 'status');
        case 'mb:release-quality':
            return processSearch('release', 'quality');
        case 'mb:release-title':
            return processSearch('release', 'title', 'release');
        case 'mb:recording-title':
            return processSearch('recording', 'title', 'recording');
        case 'mb:artist-name':
            return processSearch('artist', 'name', 'artist');
        case 'mb:area-name':
            return processSearch('area', 'name', 'area');
        case 'mb:release-language':
            return processSearch('release', 'language', 'lang');
        case 'mb:medium-format':
            return processSearch('release', 'format', 'format', 'medium');
        case 'mb:work-title':
            return processSearch('work', 'title', 'work');
        case 'mb:recording-length':
            return processSearch('recording', 'length', 'dur');
        case 'mb:release-script':
            return processSearch('release', 'script');
        case 'mb:artist-begin':
            return processSearch('artist', 'begin');
        case 'mb:area-iso1':
            return processSearch('area', 'iso1');
        case 'mb:release-date':
            return processSearch('release', 'date');
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


defineCommand("get", "retrieve triples from database", ["-s, --subject <IRI>", "-p, --predicate <IRI>", "-o, --object <IRI>"], options => {
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

    function getLiteral(name: string): string | number {
        return decodeLiteral(getPropertyFromCurrent(name));
    }

    function getString(name: string): string {
        const value = getLiteral(name);
        assertType(value, "string");
        return value as string;
    }

    function url(domain: string, path: string) {
        log(`https://${domain}/${path}`);
    }

    function resource(domain: string, type: string, predicate: string): void {
        url(domain, `${type}/${getLiteral(predicate)}`);
    }

    function mbResource(type: string): void {
        resource('musicbrainz.org', type, 'mb:mbid');
    }

    function searchBase(field: string, prefix: string, suffix: string, type: string) {
        url('musicbrainz.org', `search?query=${field}%3A${encodeURIComponent(escapeLucene(getLiteral(`mb:${prefix}-${suffix}`)))}&type=${type}&method=advanced`);
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
                let path = getString('name');


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
                        log(prepend('path'));
                        break;
                    }
                    path = prepend('name');
                }
                break;
            case 'volume':
                log(getString('path'));
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
                log(`https://musicbrainz.org/release/${getString('mb:release-id')}/disc/${getLiteral('mb:position')}`);
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

Fiber(() => {

    commander.parse(process.argv);
    if (!executed) {
        commander.outputHelp();
    }

}).run();
