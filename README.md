# Nipoppy: Parkinson's Progression Markers Initiative dataset

This repository contains code to process tabular and imaging data from the Parkinson's Progression Markers Initiative (PPMI) dataset. It is a fork of the main [Nipoppy](https://github.com/neurodatascience/nipoppy) repository. Nipoppy is a lightweight workflow management and harmonization tools for MRI and clinical data. This fork adds scripts, configuration files, and downstream analyses that are specific to PPMI.

## BIDS data file naming

<!-- TODO: update link/path once tabular is moved under workflow -->
The [tabular/ppmi_imaging_descriptions.json](https://github.com/neurodatascience/nipoppy-ppmi/blob/main/tabular/ppmi_imaging_descriptions.json) file is used to determine the BIDS datatype and suffix (contrast) associated with an image's MRI series description. It will be updated as new data is processed.

Here is a description of the available BIDS data and the tags that can appear in their filenames:

- `anat`
  - The available suffixes are: `T1w`, `T2w`, `T2starw`, and `FLAIR`
  - Most images have an `acq` tag:
    - Non-neuromelanin images: `acq-<plane><type>`, where
        - `<plane>` is one of: `sag`, `ax`, or `cor` (for sagittal, axial, or coronal scans respectively)
        - `<type>` is one of: `2D`, or `3D`
    - Neuromelanin images: `acq-NM`
  - For some images, the acquisition plane (`sag`/`ax`/`cor`) or type (`2D`/`3D`) cannot be easily obtained. In those cases, the filename will not contain an `acq` tag.
- `dwi`
  - All imaging files have the `dwi` suffix.
  - Most images have a `dir` tag corresponding to the phase-encoding direction. This is one of: `LR`, `RL`, `AP`, or `PA`
  - Images where the phase-encoding direction cannot be easily inferred from the series description string do not have a `dir` tag.
  - Some participants have multi-shell sequences for their diffusion data. These files will have an additional `acq-B<value>` tag, where `value` is the b-value for that sequence.

Currently, only structural (`anat`) and diffusion (`dwi`) MRI data are supported. Functionnal (`func`) data has not been converted to the BIDS format yet.
