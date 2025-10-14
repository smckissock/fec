export class Site {

    constructor() {
        const PARQUET_PATH = 'data/committee_contributions.parquet';
        const parquetUrl = window.location.origin + window.location.pathname.replace('index.html', '') + PARQUET_PATH;

        //const hueyUrl = `huey/index.html#autoload=${hash}`;
        //const hueyUrl = `huey/index.html#autoload=${encodeURIComponent(parquetUrl)}`;

        const hueyUrl = `huey/index.html`;

        console.log('Parquet URL:', parquetUrl);
        console.log('Huey URL:', hueyUrl);

        document.getElementById('hueyFrame').src = hueyUrl;
    }
}

new Site();