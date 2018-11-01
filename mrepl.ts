import * as assertorig from 'assert';
import * as readline from 'readline';
import * as fs from 'fs';
import * as Fiber from 'fibers';
import * as os from 'os';
import opn = require('opn');

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

function failIf(condition: boolean) {
    if (condition) {
        fail();
    }
}

Fiber(() => {

    const readFileResult = waitFor2((consumer) => fs.readFile('music.txt', 'utf8', consumer));
    //console.log(readFileResult);
    failIf(readFileResult.first !== null);
    const tasks = (readFileResult.second as string).split(os.EOL);
    //console.log(tasks);

    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
    });

    const currentTask = tasks.shift();
    failIf(currentTask === undefined);
    console.log(currentTask);
    failIf(!(currentTask as string).startsWith("https://"));
    opn(currentTask as string);
    rl.setPrompt('mscanner> ');
    rl.prompt();
    rl.on('line', (input: string) => {
        switch (input) {
            case '':
                rl.prompt();
                break;
            case 'exit':
            case 'quit':
                rl.close();
                break;
            default:
                console.log(`cannot process: ${input}`);
                rl.prompt();
        }
    });

}).run();


