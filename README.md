# Nipoppy: Parkinson's Progression Markers Initiative dataset

This repository contains code to process tabular and imaging data from the Parkinson's Progression Markers Initiative (PPMI) dataset using the [Nipoppy framework](https://nipoppy.readthedocs.io/en/stable/).

## DICOM

### PPMI data portal (LONI IDA)

* Some search fields in LONI search tool cannot be trusted
    * Examples:
        * `Modality`
            * `Modality=DTI` can have anatomical images, and there are diffusion images with `MRI` modality
        * `Weighting` (under `Imaging Protocol`)
            * Some T1s have `Weighting=PD`
    * We classify image modalities/contrast only based on the `Image Description` column
        * This can also lead to issues, for example when a subject has the same description string for all of their scans. In that case, we manually determine the image modality/contrast and hard-code the mapping in `heuristic.py` for HeuDiConv
* LONI viewer sometimes shows seemingly bad/corrupted files but they are actually fine once we convert them
    * Observed for some diffusion images (tend to have ~2700 slices according to the LONI image viewer)

### Compute Canada

* Some subjects have a huge amount of small DICOM files, which causes us to exceed the inode quota on `/scratch`
    * We opted to create SquashFS archives/filesystems, which count as 1 inode and can be mounted as a filesystem in Singularity container (using the `--overlay` argument). This is similar to [how McGill/NeuroHub stores UK Biobank data on Compute Canada](https://arxiv.org/abs/2002.06129)

## BIDS

### BIDS data file naming

<!-- TODO: update link/path once tabular is moved under workflow -->
The [tabular/ppmi_imaging_descriptions.json](https://github.com/neurodatascience/nipoppy-ppmi/blob/main/nipoppy/workflow/tabular/ppmi_imaging_descriptions.json) file is used to determine the BIDS datatype and suffix (contrast) associated with an image's MRI series description. It will be updated as new data is processed.

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

Currently, only structural (`anat`) and diffusion (`dwi`) MRI data are supported. Functional (`func`) data has not been converted to the BIDS format yet.

### HeuDiConv errors

#### Not solved yet

* `AttributeError: 'Dataset' object has no attribute 'StackID'`
    * [Vincent previously had the same issue](https://github.com/nipy/heudiconv/issues/517), unclear if/how it was fixed. Error could be because the images are in a single big DICOM instead of many small DICOM files
* `AssertionError: Conflicting study identifiers found`
    * Could be because all of a subject's DICOMs are pooled together in the `dicom_org` step, in which case this can be fixed by manually running HeuDiConv for each image
* `numpy.AxisError: axis 1 is out of bounds for array of dimension 1`
    * Only happened for one image so far
    * See https://github.com/nipy/heudiconv/issues/670 and https://github.com/nipy/nibabel/issues/1245
* `AssertionError (assert HEUDICONV_VERSION_JSON_KEY not in json_)`
    * Thrown by HeuDiConv
* `AssertionError: we do expect some files since it was called (assert bids_files, "we do expect some files since it was called")`
    * Thrown by HeuDiConv

### Notes on `dwi` data

* Some subjects only have a single diffusion image (e.g., `Ax DTI`), might not be usable
* Some subjects have 2 diffusion images, but they have the same description string (e.g., `DTI_gated`)
    * Checked some cases after BIDS conversion, and the JSON sidecars seem to have the same `PhaseEncodingDirection` (`j-`)
* Some subjects have multi-shell sequences. Their files seem to follow the following pattern:
    * `dir-PA`: 1 `B0`, 1 `B700`, 1 `B1000`, and 1 `B2000` image
    * `dir-AP`: 4 `B0` images
* Some (~2 for `ses-BL`) subjects have `dir-AP` for all their diffusion images
    * Seem to have 4 `dir-AP` `B0` images and 4 other `dir-AP` images (according to their description string)
* Some diffusion images do not contain raw data, but rather tensor model results (`FA`, `ADC`, `TRACEW`). Some of these have been excluded before BIDS conversion, but not all of them
