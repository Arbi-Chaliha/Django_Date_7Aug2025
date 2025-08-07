from django import forms

class TroubleshooterForm(forms.Form):
    # These choices will be dynamically populated in the view
    serial_number = forms.ChoiceField(
        choices=[],
        required=False,
        label="Choose a serial number",
        widget=forms.Select(attrs={'class': 'form-select'})
    )
    job_number = forms.ChoiceField(
        choices=[],
        required=False,
        label="Choose a job number",
        widget=forms.Select(attrs={'class': 'form-select'})
    )
    job_start = forms.ChoiceField(
        choices=[],
        required=False,
        label="Choose a start job",
        widget=forms.Select(attrs={'class': 'form-select'})
    )