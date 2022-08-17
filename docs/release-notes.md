## Release Notes

A non-exhaustive list of what has changed in a more readable form than a commit history.
### v1.1.1
  - Updated column reference for DRG from `drg` to `prm_drg` 

### v1.1.0
  - Beneficiaries can only have one PSP assigned per caseadmitid
    - For inpatient stays, the line with the highest allowed for the caseadmitid is assigned the psp
    - For outpatient visits, the line with the latest paid date and highest allowed for the incurred data is assigned the psp
### v1.0.1
  - Change prefix of columns from 'ccs_' to 'psp_'
  
### v1.0.0
  - Initial release of product component
  - Identify inpatient and outpatient surgeries for which multiple treatments are available.
