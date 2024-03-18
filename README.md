# tap-hubspot

`tap-hubspot` is a Singer tap for HubSpot. It extracts data from HubSpot. It leverages the HubSpot API to pull data into your data warehouse or data lake.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.


## Installation

### 1. Adding Tap-HubSpot to an Existing Meltano Project

To add the `tap-hubspot` to an existing Meltano project, follow these steps:

1. **Navigate to your Meltano project directory**:
  Open your terminal and change to the directory of your Meltano project.
  
   ```bash 
    cd your-meltano-project
   ```

2. **Add the Tap-HubSpot extractor**:
   Use the `meltano add` command to add `tap-hubspot` to your project.
   
   ```bash
   meltano add extractor tap-hubspot
   ```

   or update your `meltano.yml` file with below configuration
   ```yaml
   plugins:
      extractors:
        - name: tap-hubspot
          namespace: tap_hubspot
          pip_url: git+https://github.com/degreed-data-engineering/tap-hubspot
          config:
            start_date: "2020-03-17T00:00:00Z"
            access_token: <Access token for HubSpot API service>
            api_base_url: <Base url for the HubSpot API service>
   ```

3. **Configure the Tap-HubSpot extractor**:
   After adding the extractor, you need to configure it. You can do this interactively by running:
   
   ```bash
    meltano config tap-hubspot set --interactive
   ```
   Or, you can set the config environment variable in your .env file. For example:
   ```bash
    TAP_HUBSPOT_ACCESS_TOKEN="your_access_token_here"
    TAP_HUBSPOT_API_BASE_URL="http://api.hubapi.com"
   ```

4. **Test the Tap-HubSpot extractor configuration**:
   To ensure everything is configured correctly, test the configuration using:
   
   ```bash
    meltano config tap-hubspot test
   ```

5. **Run the Extractor**:
   Finally, run the extractor to start pulling data from HubSpot into your Meltano project. You can specify the target loader in the command. For example, if you're using `target-jsonl` as your loader:

   ```bash
    meltano run tap-hubspot target-jsonl
   ```

By following these steps, you will have successfully added `tap-hubspot` to your existing Meltano project, configured it with your HubSpot API key, and started extracting data.

### 2. Install from GitHub:

```bash
pipx install git+https://github.com/degreed-data-engineering/tap-hubspot.git
```

## Configuration

### Accepted Config Options

`tap-hubspot` requires an access token to connect and authenticate with the HubSpot APIs. This is a mandatory configurations. 

  - `access_token`: This is your HubSpot access token. 

Other configurations are
  - `api_base_url`: Base url for the HubSpot API service. Default value is `http://api.hubapi.com`
  - `start_date`: Starting point for data extraction from a source

You can set this API key in your environment variables:

```bash
export TAP_HUBSPOT_ACCESS_TOKEN=your_access_token_here
export TAP_HUBSPOT_API_BASE_URL="http://api.hubapi.com"
```

Alternatively, you can create a .env file in your project directory and add the following line:

```bash
TAP_HUBSPOT_ACCESS_TOKEN=your_access_token_here
TAP_HUBSPOT_API_BASE_URL="http://api.hubapi.com"
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Accepted Config Options

A full list of supported settings and capabilities for this tap is available by running:

```bash
tap-hubspot --about
```
Pre-Requisite tor run above command

1. Install the Tap HubSpot: If you haven't already installed the `tap-hubspot`, you need to do so. The installation method can vary depending on whether `tap-hubspot` is a standalone tool or part of a larger framework. If it's a Python package, you might use pip to install it: 

```bash
  pipx install git+https://github.com/degreed-data-engineering/tap-hubspot.git
  ```

<!-- ### Source Authentication and Authorization -->
<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-hubspot` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-hubspot --version
tap-hubspot --help
tap-hubspot --config CONFIG --discover > ./catalog.json
```

### Executing the Tap Within A Meltano Project

Use the `meltano config` command to list the settings your extractor supports:

```bash
meltano config tap-hubspot list
```
To set the appropriate values for each setting using the `meltano config` command:

```bash
meltano config tap-hubspot set <setting> <value>
```
or 

```bash
meltano config tap-hubspot set --interactive
```

If you encounter issues or need to verify the configuration, you can use the meltano config command to test the extractor settings:

```bash
meltano config tap-hubspot test
```


## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-hubspot` CLI interface directly using `poetry run`:

```bash
poetry run tap-hubspot --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-hubspot
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-hubspot --version
# OR run a test `elt` pipeline:
meltano elt tap-hubspot target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
