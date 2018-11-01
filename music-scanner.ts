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


function decodeStringLiteral(stringLiteral: string) {
    const segments = stringLiteral.split('/');
    assertEquals(segments.length, 2);
    assertEquals(segments[0], 's');
    return decodeURIComponent(segments[1]);
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

    /*
    function on(event: string, handler: any): void {
        stream.on(event, handler);
    }
    on('error', fail);
    */

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
        //console.log(compareValue.actual);
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
            //const toString = Object.getOwnPropertyDescriptor (value, "toString");
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

function expectTextTag(name: string, attributes: Attributes, value: string): Sequence<Predicate<SaxEvent>> {
    return matchTextTag(name, attributes, expectEquals(value));
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

function id(mbid: string): Attributes {
    return {
        id: mbid
    }
}

type Provider<T> = () => T;

function getStream(pattern: StatementPattern): Provider<Statement<string>> {
    return prepareDBStream(db.getStream(pattern));
}

interface NameCredit {
    readonly attributes: Attributes;
    readonly mbid: string;
    readonly name: string;
    readonly additionalTags: Sequence<Predicate<SaxEvent>>
}

function nameCredit(attributes: Attributes, mbid: string, name: string, additionalTags: Sequence<Predicate<SaxEvent>>): NameCredit {
    return {
        attributes: attributes,
        mbid: mbid,
        name: name,
        additionalTags: additionalTags
    }
}

function expectArtistCredit(nameCredits: Sequence<NameCredit>): Sequence<Predicate<SaxEvent>> {
    return expectSimpleTag(
        'artist-credit',
        map(
            nameCredits,
            nameCredit => expectTag(
                'name-credit',
                nameCredit.attributes,
                expectNamed('artist', nameCredit.mbid, nameCredit.name, nameCredit.additionalTags)
            )
        )
    );
};

const ARTIST_112 = nameCredit({}, '9132d515-dc0e-4494-85ae-20f06eed14f9', '112', undefined);

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

//const expectUnitedStates = expectArea("489ce91b-6658-3307-9877-795b68554c98", 'United States', expectUSIsoList);

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
        barcode, //matchTagAndAttributes('barcode', {}, undefined)
    ));
}

function expectRecordingCore(id: string, title: string, length: string, others: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {
    return expectTiteledEntity('recording', id, title, concat(
        expectPlainTextTag("length", length),
        others
    ));
}


const expectOfficial = expectNamedEntity('status', '4e304316-386d-3409-af2e-78857eec5cfe', 'Official');

function expectRecording(id: string, title: string, length: string): Sequence<Predicate<SaxEvent>> {
    return expectRecordingCore(id, title, length, undefined);
}

function expectRecordingExtended(id: string, title: string, length: string, tag: string, tagValue: string): Sequence<Predicate<SaxEvent>> {
    return expectRecordingCore(id, title, length, expectPlainTextTag(tag, tagValue));
}

function expectRecordingDisambiguation(id: string, title: string, length: string, tagValue: string): Sequence<Predicate<SaxEvent>> {
    return expectRecordingExtended(id, title, length, "disambiguation", tagValue);
}

function expectTagTagWithCount(count: string, name: string): Sequence<Predicate<SaxEvent>> {
    return expectTag('tag', { count: count }, expectPlainTextTag('name', name));
}

function expectTagTag(name: string): Sequence<Predicate<SaxEvent>> {
    return expectTagTagWithCount('0', name);
}

function expectPartOfRaw(mbid: string, name: string, iso1List: Sequence<Predicate<SaxEvent>>, code: string): Sequence<Predicate<SaxEvent>> {
    return expectTag('relation', {
        'type-id': "de7cc874-8b1b-3a05-8272-f3834c968fb7",
        type: "part of"
    }, concat(
        expectPlainTextTag('target', mbid),
        expectArea(mbid, name, concat(
            iso1List,
            expectIsoList('2', code)
        ))
    ))
}

function expectPartOf(mbid: string, name: string, code: string): Sequence<Predicate<SaxEvent>> {
    return expectPartOfRaw(mbid, name, undefined, code);
}

function expectPartOf2(mbid: string, name: string, code1: string, code2: string): Sequence<Predicate<SaxEvent>> {
    return expectPartOfRaw(mbid, name, expectIsoList('1', code1), code2);
}

function localExpectTextAndEnum(name: string, value: string, enumName: string, mbid: string, enumValue: string): Sequence<Predicate<SaxEvent>> {
    return concat(
        expectPlainTextTag(name, value),
        expectTextTag(enumName, id(mbid), enumValue)
    )
}

const expectDate1998 = expectDate('1998-11-16');


function expectTrue(name: string): Sequence<Predicate<SaxEvent>> {
    return expectPlainTextTag(name, 'true');
}

function expectLabel(mbid: string, name: string, additionalTags: Sequence<Predicate<SaxEvent>>): Sequence<Predicate<SaxEvent>> {
    return concat(
        expectPlainTextTag('catalog-number', '78612-73021-2'),
        expectNamed('label', mbid, name, additionalTags)
    )
}

function expectLabelAdd(mbid: string, name: string, tagName: string, tagValue: string): Sequence<Predicate<SaxEvent>> {
    return concat(
        expectPlainTextTag('catalog-number', '78612-73021-2'),
        expectNamed('label', mbid, name, expectPlainTextTag(tagName, tagValue))
    )
}

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


function tryAdd<T>(name: string, type: string, namePredicate: string, parentPredicate: string | undefined, prefix: string, linkPredicate: string | undefined, enqueued: () => T, alreadyAdded: (found: any) => T): T {

    const nameObject = `s/${encodeURIComponent(name)}`;

    const currentTask = getCurrentTask();


    function mapAttributeValues<S, T>(mapper: (subject: S, predicate: string, object: string) => T, subject: S): T[] {

        function map(predicate: string, object: string): T {
            return mapper(subject, predicate, object);
        }

        return [
            map('type', type),
            map(namePredicate, nameObject),
            ...enumOptional(parentPredicate, () => map(parentPredicate as string, currentTask))
        ];
    }

    return streamOpt(db.searchStream(mapAttributeValues(statement, db.v('s'))), () => {
        console.log(`${prefix}adding ${type} ${name}`);
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

let lastAccessedAcoustId: undefined | number = undefined;

function assertDefined(value: any): void {
    failIf(isUndefined(value));
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
        console.log(`  ${message} `);
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

    function enqueueNextTask<T, R>(items: T[], name: (item: T) => string, type: string, predicate: string, parentPredicate: string | undefined, foundResult: R, completed: () => R): R {
        for (const item of items) {
            if (enqueueUnlinkedTask(name(item), type, predicate, parentPredicate, FALSE)) {
                return foundResult;
            }
        }
        return completed();
    }

    function enqueueTasks(items: string[], type: string, predicate: string, parentPredicate: string | undefined): void {
        enqueueNextTask(items, item => item, type, predicate, parentPredicate, undefined, () => {
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
        //const allFiles: string[] = result.second;
        const files = result.second.filter(name => !name.startsWith('.'));

        //onst index = files.indexOf(".DS_Store");
        //failIf(index === -1);
        //files.splice(index, 1);
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
        return updateProperty(subject, predicate, predicatePath, value, `${literalTag} /${encodeURIComponent(value)}`, alreadyUpdated);
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

    function enqueueMBResourceTask(mbid: string, resource: string, found: () => void): void {
        enqueueMBTask(mbid, resource, undefined, undefined, found);
    }

    function getStringProperty(name: string) {
        return decodeStringLiteral(getPropertyFromCurrent(name));
    }


    function processMBResource(type: string, inner: Sequence<Predicate<SaxEvent>>, inc: string[], extraAttributes: Matcher<Attributes>, onNoMatch: () => void): void {
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
        enqueueMBResourceTask(mbid, 'area', found);
    }

    function enqueueArtist(mbid: string, found: () => void): void {
        enqueueMBResourceTask(mbid, 'artist', found);
    }

    function enqueueRecording(mbid: string, found: () => void): void {
        enqueueMBResourceTask(mbid, 'recording', found);
    }



    function adjustLiteralProperty(nameSpace: string, name: string, literalTag: string): (actualValue: any) => boolean {
        return actualValue => updateLiteralPropertyOnCurrentTask(`${nameSpace}:${name}`, `${actualValue}`, literalTag, FALSE);
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
                    //fail();
                    /*
                    if (entryName === '.DS_Store') {
                        remove('deleting file', 'unlink', entryPath);
                        return true;
                    }
                    else {
                    */
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
                            assertDefined(acoustid);
                            return enqueueUnlinkedTask(acoustid as string, 'acoustid', 'acoustid', undefined, () => {
                                const trackId = common.musicbrainz_trackid;
                                assertDefined(trackId);
                                enqueueMBResourceTask(trackId as string, 'track', fail);
                                return true;
                            });

                            /*
                            fail();

                            onNoMatch<mm.IAudioMetadata>(metaData, {
                                format: matchObject<mm.IFormat>({
                                    dataformat: expectEquals("flac"),
                                    lossless: expectEquals(true),
                                    numberOfChannels: expectEquals(2),
                                    bitsPerSample: expectEquals(16),
                                    sampleRate: checkUpdateFormatProperty('sampleRate'),
                                    duration: checkUpdateFormatProperty('duration'),
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
                            }, () => enqueueArtist("9132d515-dc0e-4494-85ae-20f06eed14f9", () => enqueueMBResourceTask("9ce47bcf-97d1-4534-b77e-b19ba6c98511", "release", fail)));
                            */
                            return true;
                        case '.jpg':
                            remove('deleting file', 'unlink', entryPath);
                            return true;
                        /*
                        fail();
                        return true;
                        */
                        //break;
                        default:
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
                    fail();
                    volumeNotMounted();
                    return false;
                }));
        case 'mb:artist':
            fail();
            processMBNamedResource('artist', expectEquals('Group'), expectEquals("e431f5f6-b5d2-343d-8b36-72607fffb74b"), expectEquals('112'), '112', concat(
                expectUsCountry,
                expectArea("489ce91b-6658-3307-9877-795b68554c98", 'United States', expectUSIsoList),
                expectAreaRaw('begin-area', "26e0e534-19ea-4645-bfb3-1aa4e83a4046", 'Atlanta', undefined),
                expectLifeSpan('1996', undefined),
                expectTag('recording-list', {
                    count: "268"
                }, concat(
                    expectRecording("00cc81c5-0dd9-45bb-a27b-ef1d5454bf85", "All Cried Out", "277000"),
                    expectRecording("1cb4f0df-21ce-4454-9346-011a5c220fec", "1's for Ya", "187000"),
                    expectRecordingExtended("3d5a22ab-2a14-4206-a15b-e1f123076927", "Anywhere", "248000", "video", "true"),
                    expectRecording("4094e83b-40a8-4494-b686-a9673be0a563", "Anything", "229093"),
                    expectRecording("454c0f69-70a4-45e9-94ce-f207848fb118", "Anywhere", "197000"),
                    expectRecording("58d482f4-7070-451b-bf0f-c5d0cd1491fa", "Anywhere (remix)", "235000"),
                    expectRecording("7001506d-45cf-491a-bf76-a4da729eff1d", "Anywhere (interlude)", "70573"),
                    expectRecording("8101a28e-36f5-482a-b947-773d605e96de", "Anywhere", "247213"),
                    expectRecording("8540c247-8043-4001-bd2a-363f750fc98f", "Anywhere", "247493"),
                    expectRecording("8e6f61af-8a46-4d5f-9b59-d31ed6b66266", "Anywhere", "182826"),
                    expectRecording("91847fac-a4ff-443b-a4cb-92cc42f2ba24", "Be With You", "244973"),
                    expectRecording("9c954ae6-c25e-4fd6-856b-7b39a8461e16", "Can I Touch You", "304573"),
                    expectRecording("a36be457-c34e-4078-84da-24894d0e34d4", "Both of Us", "219000"),
                    expectRecording("b5d0a806-3bb2-474f-828d-55806de56531", "Anywhere (Slang club remix)", "236533"),
                    expectRecording("ba519c4e-6d74-4c17-9c3a-7bbb8d6e4680", "All My Love", "261866"),
                    expectRecording("c1ac5a1a-09f2-41f0-b6ca-8ead1730c18b", "Anywhere (remix)", "280533"),
                    expectRecording("d036d54a-639a-409d-81ac-f1293bb3c004", "All I Want Is You", "221706"),
                    expectRecording("d1f1b2b5-8496-4182-91a9-5e5aeea9b291", "Call My Name", "247640"),
                    expectRecordingDisambiguation("db8b99df-ed71-4ef9-ae3c-532c192226e7", "112 Intro", "76000", "Part III version"),
                    expectRecording("e0f71717-2e40-4546-b0b6-15805d62acb4", "All Cried Out (edit)", "223426"),
                    expectRecording("e87af726-1fa3-4804-821b-741e35cb2811", "All My Love", "283173"),
                    expectRecordingDisambiguation("f8aa3a67-5368-44c5-8d9e-4433dea9fab2", "112 Intro", "132960", "original version"),
                    expectRecording("fbf9e86b-3a11-4a08-b77d-1432e1e243c9", "Anywhere", "336880"),
                    expectRecording("fc479b8c-ebbe-46f0-84cf-f45615f165cf", "After the Love Has Gone", "243000"),
                    expectRecording("ff22df38-fe40-4487-9006-fdc6a4662fe0", "Anywhere", "244026")
                ))
            ), ['recordings'], () => enqueueArea("489ce91b-6658-3307-9877-795b68554c98", () => enqueueArea("26e0e534-19ea-4645-bfb3-1aa4e83a4046", () => enqueueRecording("00cc81c5-0dd9-45bb-a27b-ef1d5454bf85", () => enqueueRecording('1cb4f0df-21ce-4454-9346-011a5c220fec', fail)))));
            return true;
        case 'mb:area':
            fail();
            processMBNamedResource('area',
                subType => getObject(currentTask, 'mb:type', FALSE, areaTypeTask => updateLiteralProperty(areaTypeTask, 'mb:name', 'mb:type/mb:name', subType, 's', fail)),
                areaTypeId => enqueueMBTask(areaTypeId, 'area-type', 'mb:type', true, (id: string) => updateProperty(currentTask, 'mb:type', 'mb:type', areaTypeId, id, fail)),
                adjustLiteralProperty('mb', 'name', 's'),
                'United States',
                concat(
                    expectUSIsoList,
                    expectTag('alias-list', {
                        count: "1"
                    }, expectTextTag('alias', {
                        'sort-name': "USA",
                        type: "Search hint",
                        'type-id': "7090dd35-e32e-3422-8a48-224821c2468b"
                    }, 'USA')),
                    expectTag('relation-list', {
                        'target-type': 'area'
                    }, concat(
                        expectPartOf('02e01cf9-b0ed-4286-ac6d-16989f92ced6', 'Virginia', 'US-VA'),
                        expectPartOf('0573177b-9ff9-4643-80bc-ed2513419267', 'Ohio', 'US-OH'),
                        expectPartOf('05f68b4c-10f3-49b5-b28c-260a1b707043', 'Massachusetts', 'US-MA'),
                        expectPartOf('0c693f90-d889-4abe-a0e6-6aac212388e3', 'New Mexico', 'US-NM'),
                        expectPartOf('10cb2ebd-1bc7-4c11-b10d-54f60c421d20', 'Wisconsin', 'US-WI'),
                        expectPartOf('1462269e-911b-4db3-be41-434393484e34', 'Missouri', 'US-MO'),
                        expectPartOf('1b420c08-51a5-4bdd-9b0e-cd601703d20b', 'Hawaii', 'US-HI'),
                        expectPartOf('1ed51cbe-4272-4df9-9b18-44b0d4714086', 'Maryland', 'US-MD'),
                        expectPartOf('2066f663-1055-4383-aaa6-08d09ec81e57', 'South Dakota', 'US-SD'),
                        expectPartOf('29fa065f-a568-418c-98b9-5023f64d9312', 'Michigan', 'US-MI'),
                        expectPartOf('373183af-56db-44d7-b06a-5877c02c5f01', 'Colorado', 'US-CO'),
                        expectPartOf('376ea713-8f27-4ab1-818b-9cca72023382', 'Oregon', 'US-OR'),
                        expectPartOf2('3906cf32-00a7-32df-93cc-4710c5f5a542', 'Puerto Rico', 'PR', 'US-PR'),
                        expectPartOf('39383cce-6f78-4afe-b19a-8377995ce702', 'Washington', 'US-WA'),
                        expectPartOf2('43dd540a-78cd-319f-bab9-214b5430f3f2', 'Guam', 'GU', 'US-GU'),
                        expectPartOf('4ca644d9-18a6-4605-9d71-3eae8b3ab2ee', 'New Hampshire', 'US-NH'),
                        expectPartOf2('4e8596fe-cbee-34ce-8b35-1f3c9bc094d6', 'United States Minor Outlying Islands', 'UM', 'US-UM'),
                        expectPartOf('6fddb177-f3fc-4c30-9d49-9c7e949fe0bc', 'Mississippi', 'US-MS'),
                        expectPartOf('75d8fdcf-03e9-43d9-9399-131b8e118b0b', 'Pennsylvania', 'US-PA'),
                        expectPartOf('75e398a3-5f3f-4224-9cd8-0fe44715bc95', 'New York', 'US-NY'),
                        expectPartOf('7a0e4090-2ab5-4a28-acef-6173e3885fa7', 'Delaware', 'US-DE'),
                        expectPartOf('7deb769c-1eaa-4b7a-aecf-c395d82a1e73', 'Utah', 'US-UT'),
                        expectPartOf('821b0738-e1a2-4636-82e0-b5ca8b331679', 'Alaska', 'US-AK'),
                        expectPartOf('85255cb8-edb9-4a66-b23a-a5261d42c116', 'Kentucky', 'US-KY'),
                        expectPartOf('8788d6c2-c779-4be5-ad47-cf0a95e0f2a0', 'Arkansas', 'US-AR'),
                        expectPartOf('88772016-5866-496a-8de7-4340e922d663', 'Connecticut', 'US-CT'),
                        expectPartOf('8c2196d9-b7be-4051-90d1-ac81895355f1', 'Illinois', 'US-IL'),
                        expectPartOf('8c3615bc-bd11-4bf0-b237-405161aac8b7', 'Iowa', 'US-IA'),
                        expectPartOf2('9a84fea2-1c1f-3908-a44a-6fa2b6fa7b26', 'Northern Mariana Islands', 'MP', 'US-MP'),
                        expectPartOf('a3435b4a-f42c-404e-beee-f290f62a5e1c', 'Vermont', 'US-VT'),
                        expectPartOf('a36544c1-cb40-4f44-9e0e-7a5a69e403a8', 'New Jersey', 'US-NJ'),
                        expectPartOf('a5ff428a-ad62-4752-8f8d-14107c574117', 'Nebraska', 'US-NE'),
                        expectPartOf('ab47b3b2-838d-463c-9907-30dcd3438d65', 'Nevada', 'US-NV'),
                        expectPartOf('ae0110b6-13d4-4998-9116-5b926287aa23', 'California', 'US-CA'),
                        expectPartOf('aec173a2-0f12-489e-812b-7d2c252e4b62', 'South Carolina', 'US-SC'),
                        expectPartOf('af4758fa-92d7-4f49-ac74-f58d3113c7c5', 'North Dakota', 'US-ND'),
                        expectPartOf('af59135f-38b5-4ea4-b4e2-dd28c5f0bad7', 'Washington, D.C.', 'US-DC'),
                        expectPartOf('b8c5f945-678b-43eb-a77a-f237d7f01493', 'Rhode Island', 'US-RI'),
                        expectPartOf('bb32d812-8161-44e1-8a73-7a0d4a6d3f96', 'West Virginia', 'US-WV'),
                        expectPartOf('bf9353d8-da52-4fd9-8645-52b2349b4914', 'Arizona', 'US-AZ'),
                        expectPartOf('c2dca60c-5a5f-43b9-8591-3d4e454cac4e', 'Wyoming', 'US-WY'),
                        expectPartOf('c45232cf-5848-45d7-84ae-94755f8fe37e', 'Maine', 'US-ME'),
                        expectPartOf('c747e5a9-3ac7-4dfb-888f-193ff598c62f', 'Kansas', 'US-KS'),
                        expectPartOf('cc55c78b-15c9-45dd-8ff4-4a212c54eff3', 'Indiana', 'US-IN'),
                        expectPartOf('cffc0190-1aa2-489f-b6f9-43b9a9e01a91', 'Alabama', 'US-AL'),
                        expectPartOf('d10ba752-c9ce-4804-afc0-7ff94aa5d8d6', 'Georgia', 'US-GA'),
                        expectPartOf('d2083d84-09e2-4d45-8fc0-45eed33748b5', 'Oklahoma', 'US-OK'),
                        expectPartOf('d2918f1a-c51e-4a4a-ad7f-cdd88877b25f', 'Florida', 'US-FL'),
                        expectPartOf('d4ab49e7-1d25-45e2-8659-b147e0ea3684', 'North Carolina', 'US-NC'),
                        expectPartOf2('e228a3c1-53c0-3ec9-842b-ec1b2138e387', 'American Samoa', 'AS', 'US-AS'),
                        expectPartOf('f2532a8e-276c-457a-b3d9-0a7706535178', 'Idaho', 'US-ID'),
                        expectPartOf('f5ffcc03-ebf2-466a-bb11-b38c6c0c84f5', 'Minnesota', 'US-MN'),
                        expectPartOf('f934c8da-e40e-4056-8f8c-212e68fdcaec', 'Texas', 'US-TX'),
                        expectPartOf('f9caf2d8-9638-4b96-bc49-8462339d4b2e', 'Tennessee', 'US-TN'),
                        expectPartOf('fb8840b9-ff2f-4484-8540-7112ee426ea7', 'Montana', 'US-MT'),
                        expectPartOf('fc68ecf5-507e-4012-b60b-d93747a3cfa7', 'Louisiana', 'US-LA')
                    )),
                    expectTag('tag-list', {}, concat(
                        expectTagTag('fail'),
                        expectTagTag('place'),
                        expectTagTagWithCount('-1', 'the tag voters have no sense of humour. vote either fail or whatever as an answer!'),
                        expectTagTag('un member state'),
                        expectTagTag('united states of what?'),
                        expectTagTag('vote either fail or whatever as an answer! united states of what??'),
                        expectTagTag('whatever')
                    ))
                ), ['aliases', 'annotation', 'tags', 'ratings', 'area-rels'], () => enqueueArea('02e01cf9-b0ed-4286-ac6d-16989f92ced6', fail));
            return true;
        case 'mb:release':
            fail();

            processMBResource('release', concat(

                localExpectTextAndEnum('title', 'Room 112', 'status', "4e304316-386d-3409-af2e-78857eec5cfe", 'Official'),
                localExpectTextAndEnum('quality', 'normal', 'packaging', "ec27701a-4a22-37f4-bfac-6616e0f9750a", 'Jewel Case'),
                expectTextRepresentation,

                expectArtistCredit(sequence(ARTIST_112)),


                expectDate1998,
                expectCountry('DE'),

                expectReleaseEventList([concat(
                    expectDate1998,
                    expectArea('85752fda-13c4-31a3-bee5-0e5cb1f51dad', 'Germany', expectIsoList1('DE'))
                )]),


                expectPlainTextTag('barcode', '786127302127'),
                expectPlainTextTag('asin', 'B00000D9VN'),
                expectSimpleTag('cover-art-archive', concat(
                    expectTrue('artwork'),
                    expectPlainTextTag('count', '19'),
                    expectTrue('front'),
                    expectTrue('back')
                )),

                expectList('label-info', [
                    expectLabelAdd('c62e3985-6370-446a-bfb8-f1f6122e9c33', 'Arista', 'label-code', '3484'),
                    expectLabel('29d43312-a8ed-4d7b-9f4e-f5650318aebb', 'Bad Boy Records', undefined),
                    expectLabelAdd('29d7c88f-5200-4418-a683-5c94ea032e38', 'BMG', 'disambiguation', 'the former Bertelsmann Music Group, defunct since 2004-08-05; for releases dated 2008 and later, use "BMG Rights Management"')
                ])
            ), ['artists', 'collections', 'labels'], {}, () => enqueueArtist('9132d515-dc0e-4494-85ae-20f06eed14f9', () => enqueueArea('85752fda-13c4-31a3-bee5-0e5cb1f51dad', () => enqueueMBResourceTask('c62e3985-6370-446a-bfb8-f1f6122e9c33', 'label', fail))));
            return true;
        /*
        case 'mb:area-type':
            const nextSubject = getStream({ predicate: 'mb:type', object: currentTask });
            const statement = nextSubject();
            assert(isDefined(statement));
            const nextPlayList = getStream({ subject: statement.subject, predicate: 'playlist' });
            assertUndefined(nextPlayList());
            assertUndefined(nextSubject());
            //fail();
            moveToNext();
            return true;
        */
        case 'mb:recording':
            fail();
            processMBResource('recording', concat(
                expectPlainTextTag('title', 'All Cried Out'),
                expectPlainTextTag('length', '277000'),
                expectArtistCredit(sequence(nameCredit({ joinphrase: " duet with " }, 'fedd7b2a-bda1-4914-984a-e66fb4f2a561', 'Allure', expectPlainTextTag('disambiguation', 'female R&B group')), ARTIST_112))
            ), ['artists'], {}, () => enqueueArtist('fedd7b2a-bda1-4914-984a-e66fb4f2a561', fail));
            //fail();
            return true;
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
            assert(isUndefined(lastAccessedAcoustId));
            lastAccessedAcoustId = Date.now();
            const resourcePath = `/v2/lookup?client=0mgRxc969N&meta=recordingids&trackid=${acoustid}`;
            console.log(`https://acoustid.org/track/${acoustid}`);
            const run = getRunner();
            https.get({
                hostname: 'api.acoustid.org',
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
            const result = JSON.parse(response);
            assertEquals(result.status, "ok");
            const results = result.results;
            assertEquals(results.length, 1);
            const firstResult = results[0];
            assertEquals(firstResult.id, acoustid);
            return enqueueNextTask(firstResult.recordings, recording => (recording as any).id, 'mb:recording', 'mb:mbid', undefined, true, fail);
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

    function resource(domain: string, type: string, predicate: string): void {
        console.log(`https://${domain}/${type}/${getStringProperty(predicate)}`);
    }

    function mbResource(type: string): void {
        resource('musicbrainz.org', type, 'mb:mbid');
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
            default:

                fail();
                break;
        }
        //console.log(type === 'root' ? '<root>' : decodeStringLiteral(getPropertyFromCurrent(type)));
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
