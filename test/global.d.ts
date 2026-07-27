/** mocha-cakes-2 BDD UI and chai expect globals, see .mocharc.json */

declare const expect: (typeof import('chai'))['expect'];

declare function Feature(title: string, fn: () => void): void;
declare function Scenario(title: string, fn: () => void): void;
declare function Given(title: string, fn?: Mocha.Func | Mocha.AsyncFunc): void;
declare function When(title: string, fn?: Mocha.Func | Mocha.AsyncFunc): void;
declare function Then(title: string, fn?: Mocha.Func | Mocha.AsyncFunc): void;
declare function And(title: string, fn?: Mocha.Func | Mocha.AsyncFunc): void;
declare function But(title: string, fn?: Mocha.Func | Mocha.AsyncFunc): void;
declare function beforeEachScenario(fn: Mocha.Func | Mocha.AsyncFunc): void;
declare function afterEachScenario(fn: Mocha.Func | Mocha.AsyncFunc): void;
